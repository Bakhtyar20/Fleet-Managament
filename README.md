The PDM_Fleet_monthly_refresh Notebook serves as a monthly processing and data refresh task focused on maintaining and updating fleet-related data. Here’s a breakdown of its purpose and functionality in business terms:

1. Monthly Data Aggregation and Preparation
Each month, the notebook aggregates and prepares data specific to the fleet, which includes details like vehicle usage, availability, cost information, and other key metrics.
It integrates data from various internal and external sources, ensuring that the latest information is available for fleet analysis. This could involve data on vehicle usage patterns, maintenance schedules, and leasing or ownership costs.
2. Holiday and Working Day Adjustments
Using a holiday library, the notebook identifies non-working days and public holidays in specific countries.
This is important for accurate reporting and processing. For example, if data processing depends on business days (e.g., only updating certain records on working days), the notebook can adjust accordingly, helping to avoid delays or errors in reports and calculations.
3. Execution Trigger Based on Workday Conditions
The notebook determines which processing steps to execute based on the business day. It uses a parameter called run_sql_out to decide whether it’s the first, second, or third working day, or a non-working day.
This approach allows the notebook to handle different actions depending on the timing within the business month. For example:
First or Second Working Day: Standard data preparation, used by regular reports or daily dashboard updates.
Third Working Day: Special processing for additional data that may be required for compliance or month-end reporting, such as generating reports for external stakeholders or conducting more complex analyses.
Non-Working Day or Special Condition: Pauses or skips certain data operations, optimizing resources and ensuring that data isn’t processed redundantly or inaccurately.
4. Integration with Databricks
This notebook is configured to run on Databricks, a platform that enables large-scale data processing. Databricks connects directly to databases and data lakes, allowing the notebook to perform high-speed calculations and handle large datasets efficiently.
Libraries like tabulate, holidays, and mailjet_rest are added for various functionalities:
Tabulate: For creating clean tables and summaries of data, making it easier to review results.
Holidays: For identifying public holidays, used in determining working and non-working days.
Mailjet: For sending automated notifications or alerts based on the results of the notebook’s execution.
5. Error Monitoring and Alerting
If any issues arise during the notebook’s execution—such as data inconsistency, missing information, or system errors—an alert system automatically sends an email to designated team members.
These emails ensure prompt notification of errors so that any issues can be addressed quickly. The subject and content of the emails are tailored to the nature of the error, allowing the team to prioritize and respond efficiently.
6. Output for Other Downstream Processes
Once the data processing is completed, the notebook stores the updated data in a way that it can be accessed by other data pipelines or business intelligence tools.
This data is then available for further processes, like financial reporting, operational decision-making, and performance analysis of the fleet. It may also feed into dashboards and other visualizations used by stakeholders to monitor fleet performance.
7. Business Impact and Purpose
Overall, the PDM_Fleet_monthly_refresh notebook supports fleet management by ensuring that up-to-date, accurate, and actionable data is available on a monthly basis.
The notebook’s automated workflows reduce manual effort, improve data quality, and help ensure timely reporting and analytics, which can inform strategic decisions on fleet usage, maintenance, and investments.
This monthly refresh process is especially valuable for a business with large fleets, where even minor improvements in efficiency can lead to substantial cost savings and improved customer service.


first_country_list, second_country_list, and third_country_list contain subsidiary list as mentioned above are grouped for processing the fleet data refresh.
This notebook triggers the execution of two other Databricks notebooks:
PDM FLEET: This notebook is called for each subsidiary in the refresh list, performing the main fleet data refresh process.
PDM_Masking: This notebook applies data masking to the refreshed data, preparing it for secure storage in SQL Server.
These notebooks are dynamically run depending on specific conditions, such as the success or failure of the data refresh and masking requirements.

This Databricks notebook facilitates the automated monthly refresh of fleet data for the Pricing Data Mart (PDM), ensuring accurate, up-to-date information across subsidiaries. It identifies subsidiaries for refresh based on predefined country lists, schedules operations according to the working days of each month, and checks the currency of fleet reports before processing. Data snapshots are taken for the current, previous, and two-month-old files, ensuring robust data retention. If data refresh fails for any subsidiary, alerts are generated and sent to relevant stakeholders. Additionally, the notebook applies data masking for security before storage in SQL Server. By automating these tasks, the notebook enhances operational efficiency, supports compliance, and ensures accurate data availability for business insights

after fleet refresh:
Data Snapshots and File Management:

Based on the date, snapshots of fleet tables (PDM_FLEET_T_PREV_MTH and PDM_FLEET_T_2_MTH_OLD) are created by copying files. This step keeps track of the current, previous, and two-month-old snapshots.

At the end of this notebook, a JSON dump is created containing key output parameters (run_sql_out) which signal the status of the refresh. This JSON file is then used by an external pipeline (likely in Azure Data Factory) to trigger subsequent processes based on the notebook's outcome. This setup ensures seamless, automated integration with the larger data processing workflow.


# Fleet-Managament[PDM_FLEET_MV.zip](https://github.com/user-attachments/files/17541323/PDM_FLEET_MV.zip)



The PDM_Fleet_monthly_refresh Notebook is a critical tool for our fleet management operations, designed to ensure that our fleet-related data is consistently updated and accurate on a monthly basis. Each month, it aggregates and prepares data from various sources, including vehicle usage, availability, and cost information, to provide a comprehensive view of our fleet's performance. The notebook adjusts for public holidays and non-working days to ensure accurate reporting and processing, and it triggers specific actions based on the business day, optimizing our data operations. This is important for accurate reporting and processing. For example, if data processing depends on business days (e.g., only updating certain records on working days), the notebook can adjust accordingly, helping to avoid delays or errors in reports and calculations. It uses a parameter called run_sql_out to decide whether it’s the first, second, or third working day, or a non-working day.
This approach allows the notebook to handle different actions depending on the timing within the business month. For example:
First or Second Working Day:
First List of subsidiaries is processed which includes 
ALD Automotive Austria
ALD Automotive Brazil
ALD Automotive Croatia
ALD Automotive Czech Republic
ALD Automotive Denmark
Nordea Denmark
ALD Automotive Estonia
ALD Automotive Finland
Nordea Finland
Ayvens France
Ayvens France Bremany
Parcours France
ALD Automotive Germany
CPM Fleet Management Germany
ALD Automotive Hungary
ALD Automotive Lithuania
ALD Automotive Mexico
ALD Automotive Netherlands
Ford Fleet Management Netherlands
Nordea Norway
ALD Automotive Spain
Soremo Spain
ALD Automotive Sweden
Nordea Sweden
ALD Automotive Switzerland

Second Day :
ALD Automotive Algeria
ALD Automotive Belgium
ALD Automotive Bulgaria
ALD Automotive Belarus
ALD Automotive Chile
ALD Automotive Colombia
ALD Automotive India
ALD Automotive Italy
ALD Automotive Luxembourg
ALD Automotive Morocco
ALD Automotive Poland
ALD Automotive Romania
ALD Automotive Turkey
ALD Automotive Ukraine

Third Day:

ALD Automotive Greece
ALD Automotive Peru
ALD Automotive United Kingdom
Ford Fleet Management UK

 First Day:Standard data preparation, used by regular reports or daily dashboard updates.
Third Working Day: Special processing for additional data that may be required for compliance or month-end reporting, such as generating reports for external stakeholders or conducting more complex analyses.
Non-Working Day or Special Condition: Pauses or skips certain data operations, optimizing resources and ensuring that data isn’t processed redundantly or inaccurately.

first_country_list, second_country_list, and third_country_list contain subsidiary list as mentioned above are grouped for processing the fleet data refresh.
This notebook triggers the execution of two other Databricks notebooks:
PDM FLEET: This notebook is called for each subsidiary in the refresh list, performing the main fleet data refresh process.
PDM_Masking: This notebook applies data masking to the refreshed data, preparing it for secure storage in SQL Server.
These notebooks are dynamically run depending on specific conditions, such as the success or failure of the data refresh and masking requirements.

PDM FLeet notebook facilitates the automated monthly refresh of fleet data for the Pricing Data Mart (PDM), ensuring accurate, up-to-date information across subsidiaries. It identifies subsidiaries for refresh based on predefined country lists, schedules operations according to the working days of each month, and checks the currency of fleet reports before processing. Data snapshots are taken for the current, previous, and two-month-old files, ensuring robust data retention. If data refresh fails for any subsidiary, alerts are generated and sent to relevant stakeholders. Additionally, the notebook applies data masking for security before storage in SQL Server. By automating these tasks, the notebook enhances operational efficiency, supports compliance, and ensures accurate data availability for business insights

after fleet refresh:
Data Snapshots and File Management:

Based on the date, snapshots of fleet tables (PDM_FLEET_T_PREV_MTH and PDM_FLEET_T_2_MTH_OLD) are created by copying files. This step keeps track of the current, previous, and two-month-old snapshots.

At the end of this notebook, a JSON dump is created containing key output parameters (run_sql_out) which signal the status of the refresh. This JSON file is then used by an external pipeline (likely in Azure Data Factory) to trigger subsequent processes based on the notebook's outcome. This setup ensures seamless, automated integration with the larger data processing workflow.

[Pricing (1).zip](https://github.com/user-attachments/files/17556890/Pricing.1.zip)
