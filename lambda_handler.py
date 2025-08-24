def lambda_handler(event, context):
    PopulateWithRedditScrape()
    print("Lambda file executed!")