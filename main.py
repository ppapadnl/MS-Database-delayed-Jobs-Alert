from datetime import timedelta
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import pandas as pd
import time
from datetime import datetime
import base64
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.mime.text import MIMEText

conn_str = (
    r'Driver=SQL Server;'
    r'Server=SRVurl,Port;'
    r'Database=DBNAME;'
    r'UID=USERNAME;'
    r'PWD=PASSWORD'
)

# Involving sqlalchemy in Order to align with pandas DOC
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
engine = create_engine(connection_url)
EXCLUSION_COMPANY = ["Bedrijf Test", "CRONUS Nederland BV", "Template 2015 (6) Productie",
                     "Heinsberg", "NL-test", "Template Company", "Buddel"]
# BC DB MS data

mysql = """
SELECT * FROM [BC_LIVE].[dbo].[Company]
"""


def fetch_companies():
    with engine.connect() as conn:
        df = pd.read_sql(mysql, conn)
    return df.iloc[:, 1].tolist()


def fetch_job_queue_entries(company_name):
    mysql2 = f"""
        SELECT [ID], 
        [User ID],
        COALESCE(NULLIF([Description], ''), 
        CAST([Object ID to Run] AS VARCHAR) + ' ') AS TaskDescription,
        [Last Ready State], 
        [Expiration Date_Time],
        [Earliest Start Date_Time], 
        [Object Type to Run], 
        [Object ID to Run],
        [Status], 
        [No_ of Minutes between Runs]
        FROM [RWSNL_BC_LIVE].[dbo].[{company_name}$Job Queue Entry$437dbf0e-84ff-417a-965d-ed2bb9650972]
        WHERE [Status] <> 3 
        AND [Object ID to Run] NOT LIKE '1509%'
        ORDER BY [Earliest Start Date_Time]
        """

    with engine.connect() as conn:
        df = pd.read_sql(mysql2, conn)
    return df


def check_job_entries():
    log_data = []  # Store logs for Excel output
    companies = fetch_companies()

    for company_name in companies:
        if company_name not in EXCLUSION_COMPANY:
            df_jobs = fetch_job_queue_entries(company_name)

            if not df_jobs.empty:
                df_jobs["Delayed"] = df_jobs["Earliest Start Date_Time"] < datetime.utcnow() - timedelta(
                    seconds=121)  # Check why 121 seconds
                df_delayed = df_jobs[df_jobs["Delayed"]]

                for _, row in df_delayed.iterrows():
                    attn_message = {
                        "Company": company_name,
                        "Task Description": row["TaskDescription"],
                        "Last Planned Start Date": row["Earliest Start Date_Time"].strftime("%Y-%m-%d %H:%M")}
                    log_data.append(attn_message)
                    print(f"Company [{attn_message['Company']}] - [{attn_message['Task Description']}] "
                          f"Last planned start date: {attn_message['Last Planned Start Date']}")

    if log_data:
        # Convert log data to DataFrame and save to Excel
        df_log = pd.DataFrame(log_data)
        file_path = r"C:\Users\NLTLG03.pypower\Documents\PythonProjects\BC_JOB_ALERT\BC_JOB_ALERT.xlsx"
        df_log.to_excel(file_path, index=False)
        print(f"\nLog saved to {file_path}")
    else:
        print("No Delayed Job Queue Entries Found.")
        exit()


check_job_entries()

time.sleep(1)
strDate = datetime.today().strftime('%d-%m-%Y')
filename = r"C:\Users\Pypower\Documents\PythonProjects\BC_JOB_ALERT\BC_JOB_ALERT.xlsx"
image_file = open(r"C:\Users\Pypower\Documents\PythonProjects\Project\Logo.png", 'rb').read()
encoded_image = base64.b64encode(image_file).decode("utf-8")

from_email = "MS_BC_DoNotReply@nl.rhenus.com"
to_emails = ["receipients@fmail.com"]
cc_emails = ["receipients@fmail.com"]

subject = "Job Queue Errors in Business Central  " + strDate
message = '<p3>Hello,  </p3>' '<br>' '<br>' '<br>' \
          '<p3>A script was run for all companies within Business Central to identify job queues that are in error. ' '<br>' \
          ' The attached file contains the company(ies) with errors. </p3>''<br>''<br>' '<br>''<br>''<b>Action Required: ' '</b>'\
          '<br>' \
          '<p3>Please review the error(s) and take corrective action or inform the Finance department users as necessary. ' \
          '</p3>''<br>''<br>''<br>' \
          '<p3>Best regards,</p3>' \
          '<h5 style="color: red">IT Team</h5>' \
          '<img src="data:image/png;base64,%s"/>' % encoded_image

attachmentEx = open(filename, 'rb')
attachment_filename = os.path.basename(filename)
msg = MIMEMultipart()
msg['To'] = ", ".join(to_emails)
msg['Cc'] = ", ".join(cc_emails)
msg['Subject'] = subject
msg.attach(MIMEText(message, 'html'))
msg["Importance"] = "high"
attachment = MIMEBase('application', 'octet-stream')
attachment.set_payload(attachmentEx.read())
encoders.encode_base64(attachment)
attachment.add_header('Content-Disposition', f'attachment; filename={attachment_filename}')
msg.attach(attachment)
server = smtplib.SMTP('smtp.service.ca', 25)
server.sendmail(from_email, to_emails + cc_emails, msg.as_string())
print("mail sent")
server.quit()
exit()
