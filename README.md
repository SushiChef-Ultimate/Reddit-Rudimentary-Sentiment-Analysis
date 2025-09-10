Project Outcomes:
    Implemented an automated daily ETL pipeline that ingests 100 Reddit posts per day from the subreddit r/economics (via Reddit's public API) into a PostgreSQL database, with a Power BI report built on top to visualize the processed data.


Hello viewers!

This README will serve mostly as documentation of my learning and thought process as I was creating this personal project (and somewhat as a display of technical knowledge).

This project was at first meant to be a very simple exercise in scraping publicly available data (I settled on scraping Reddit because they provide a free API) and dumping the data in a database after transforming it. However, as the project progressed, I began to see more and more potential in the project and decided that I wanted to leverage what I learned about cloud to make this project a cloud-handled and automated ETL pipeline.

I initially implemented this as an AWS CI/CD pipeline integrated with my GitHub repo. I gained some pretty thorough hands-on experience with CodePipeline, CodeBuild, AWS Lambda, Glue, and even IAM. While AWS offered a lot of really powerful and convenient solutions, I realized that for this project (especially integrating with Power BI for reporting) Azure provided me a more streamlined path. While evaluating these two platforms, I figured that Azure may be able to support this specific project more than AWS. That said, the AWS experience was invaluable for understanding how to design scalable pipelines and manage cloud resources.

Implementing this in Azure I found to be a lot more intuitive than on AWS. Azure Functions has a built-in capability to continuously deploy with GitHub, which I found to be incredibly convenient. From there, I connected my GitHub with Azure Functions, stored my secrets in the key vault, made sure my Python file could connect to the Reddit API and PostgreSQL database I provisioned, and set up the automation.

I'm working within the bounds of an Azure student account, so I was faced with pricing decisions as I was provisioning my resources. For example, when provisioning my database, I had to turn off Availability Zones in order to drive down costs. While this is fine in a personal project environment, this probably wouldn't fly in a formal production environment, in which case zone/geo redundancy becomes critical for uptime.

I also configured my function to only operate once a day. The Reddit API only allows up to 100 rows to be retrieved per call. This means my pipeline is populating the database with only 100 records per day. Far below the volumes of professional projects, but this small runtime helps to drive down costs and stay within credit limits, while also yielding some very interesting insights into the nature of current economic discussions on social media (see data-insights.txt for more).

One issue I ran into was constant ImportModuleErrors, but after looking through some documentation I discovered a solution. Since the Pandas and NumPy libraries can be somewhat heavy, it's usually best to utilize remote deploy to Azure so that Azure can handle downloading dependencies on deply instead of pre-packaged. This usually works best on a Linux instance, especially premium, since Linux leverages Oryx to download these hefty libraries a little more smoothly.

As for my modeling using Power BI, I opted at first to use DirectQuery in order to have Power BI stay linked with the state of my database at all times by writing queries to the DB instead of importing that data; however, I soon switched to imports.

The reason for this is that Power BI DirectQuery doesn't support all the queries and custom columns I wanted to create, so imports ended up being the better pick.

As of writing, my Power BI report is being hosted on Power BI Service, but since the school trial capacity of Power BI doesn't enable web sharing, I unfortunately had to opt for downloading a PDF of my report (titled Reddit-Scrape-ImportModel.pdf). This intial version will account for 100 distinct records, but ill also continously update my GitHub with a more recent version (titled Reddit-Scrape-LatestImport.pdf)

Thanks for your attention!
