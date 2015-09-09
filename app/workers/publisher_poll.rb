require 'app/services/connection_builder'
require 'app/services/publisher_update'
require 'pony'

module Citygram::Workers
  class PublisherPoll
    include Sidekiq::Worker
    sidekiq_options retry: 5

    MAX_PAGE_NUMBER = 10
    NEXT_PAGE_HEADER = 'Next-Page'.freeze

    def perform(publisher_id, url, page_number = 1)
      # fetch publisher record or raise
      publisher = Publisher.first!(id: publisher_id)

      # prepare a connection for the given url
      connection = Citygram::Services::ConnectionBuilder.json("request.publisher.#{publisher.id}", url: url)

      # execute the request or raise
      response = connection.get

      # save any new events
      feature_collection = response.body
      new_events = Citygram::Services::PublisherUpdate.call(feature_collection.fetch('features'), publisher)

      # OPTIONAL PAGINATION:
      #
      # iff successful to this point, and a next page is given
      # queue up a job to retrieve the next page
      #
      next_page = response.headers[NEXT_PAGE_HEADER]
      if new_events.any? && valid_next_page?(next_page, url) && page_number < MAX_PAGE_NUMBER
        self.class.perform_async(publisher_id, next_page, page_number + 1)
      end
    end

    sidekiq_retries_exhausted do |msg|
      Sidekiq.logger.warn "Failed #{msg['class']} with #{msg['args']}: #{msg['error_message']}"
      # send message to the publisher owner / contact
      notify_publisher_contact(publisher)
    end

    private

    def valid_next_page?(next_page, current_page)
      return false unless next_page.present?

      next_page = URI.parse(next_page)
      current_page = URI.parse(current_page)

      next_page.host == current_page.host
    end

    def notify_publisher_contact(publisher_info)
      Pony.options = {
        from: ENV.fetch('SMTP_FROM_ADDRESS'),
        via: :smtp,
        via_options: {
          enable_starttls_auto: true,
          authentication: :plain,
          address:        ENV.fetch('SMTP_ADDRESS'),
          port:           ENV.fetch('SMTP_PORT'),
          user_name:      ENV.fetch('SMTP_USER_NAME'),
          password:       ENV.fetch('SMTP_PASSWORD'),
          domain:         ENV.fetch('SMTP_DOMAIN'),
        }
      }

      body = "Notification Error.<hr>
              Citygram has unsuccessfully tried to connect to your endpoint. <br>
              Description: #{publisher_info.description}. <br>
              Endpoint: #{publisher_info.endpoint}."

      Pony.mail(
        to: publisher_info.email, # TODO: need to add an email contact to the publisher endpoint.
        subject: "Citygram publisher - endpoint error notification",
        html_body: body,
      )
    end
  end
end
