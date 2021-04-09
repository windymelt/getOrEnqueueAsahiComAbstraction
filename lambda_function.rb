require 'json'
require 'aws-sdk-dynamodb'
require 'aws-sdk-sqs'


TABLE_NAME = 'asahi_abstraction_status'
SQS_URL = ENV['SQS_QUEUE_URL']
    
def lambda_handler(event:, context:)
    dynamodb = Aws::DynamoDB::Client.new(region: 'ap-northeast-1')
    sqs = Aws::SQS::Client.new(region: 'ap-northeast-1')

    url = event['body']
    
    re = Regexp.new('^https://blog\.3qe\.us/.+')
    if ! re.match?(url)
        return { statusCode: 400, body: JSON.generate("invalid url") }
    end

    previewre = Regexp.new('^https://blog\.3qe\.us/preview$')
    if previewre.match?(url)
        return { statusCode: 400, body: JSON.generate("invalid url") }
    end

    params = {
        table_name: TABLE_NAME,
        key: {
            url: url
        }
    }
    begin
        result = dynamodb.get_item(params)

        if result.item == nil
            puts 'Could not find url'
            # enqueue
            new_status(dynamodb: dynamodb, url: url, status: "enqueued")
            enqueue_to_sqs(sqs: sqs, url: url)
            return { statusCode: 200, body: JSON.generate({abstract: "<Preparing abstract>"}) }
        end

    puts 'found table:'
    puts result.item
    
    if result.item['status'] == "enqueued"
        return { statusCode: 200, body: JSON.generate({abstract: "<Preparing abstract>"})}
    end
    
    if result.item['status'] == "done"
        return { statusCode: 200, body: JSON.generate({abstract: result.item['abstract']})}
    end
    rescue  Aws::DynamoDB::Errors::ServiceError => error
      puts 'Unable to find url:'
      puts error.message
    end
    { statusCode: 400, body: JSON.generate({abstract: "unknown request"}) }
end

def new_status(dynamodb:, url:, status:)
    params = {
        table_name: TABLE_NAME,
        item: {
            url: url,
            status: status
        }
    }
    begin
        dynamodb.put_item(params)
    rescue  Aws::DynamoDB::Errors::ServiceError => error
        puts 'Unable to add url:'
        puts error.message
    end
    true
end

def enqueue_to_sqs(sqs:, url:)
    sqs.send_message(queue_url: SQS_URL, message_body: url)
    true
end

