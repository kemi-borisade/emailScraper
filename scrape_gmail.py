# Importing libraries
import imaplib
import email
import yaml  # To load saved login credentials from a yaml file
import pandas as pd
from datetime import datetime
from datetime import timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import logging
import logging.handlers


# Create and configure logger
#logging.basicConfig(filename="std.log", format='%(asctime)s %(message)s', filemode='w')
#logger=logging.getLogger()
#logger.setLevel(logging.DEBUG)




def log_on_fetch(file):
    with open(file) as f:
        content = f.read()
    # import saved credentials
    credentials = yaml.load(content, Loader=yaml.FullLoader)
    user, password = credentials["user"], credentials["password"]
    # URL for IMAP connection . IMAP allows the download of emails from the gmail server

    imap_url = 'imap.gmail.com'
    # Connection with GMAIL using SSL
    my_mail = imaplib.IMAP4_SSL(imap_url)
    my_mail.login(user, password)
    # Fetch email from the inbox.
    my_mail.select('Inbox')

    datesince = (datetime.today() - timedelta(8)).strftime("%d-%b-%Y")
    _, data = my_mail.search(None,
                             f'(FROM "notify@payments.interac.ca" BODY "automatically deposited into your bank account at TD Canada Trust" SENTSINCE {datesince})')
    mail_id_list = data[0].split()  # IDs of all emails that we want to fetch
    msgs = []  # empty list to capture all messages
    # Iterate through messages and extract data into the msgs list
    for num in mail_id_list:
        typ, data = my_mail.fetch(num, '(RFC822)')  # RFC822 returns whole message (BODY fetches just body)
        msgs.append(data)
   # logger.info("logged on and fetched messages successfully")
    return msgs


def append_msgs(msgs):
    # empty arrays to append several parts of the email
    date_list = []
    from_list = []
    subject_text = []
    body_text = []
    sender_email = []

    for msg in msgs[::-1]:
        for response_part in msg:
            if type(response_part) is tuple:
                my_msg = email.message_from_bytes((response_part[1]))
                #date_list.append(my_msg.values()[12])
                date_list.append(my_msg ['Date'])

                subject_t = my_msg['subject']
                subject_text.append(subject_t)

                fromlist = my_msg['From']
                from_list.append(fromlist)

                senderemail = my_msg['Reply-To']
                sender_email.append(senderemail)

                for part in my_msg.walk():
                    # print(part.get_content_type())
                    if part.get_content_type() == 'text/plain':
                        body_t = part.get_payload()
                        body_text.append(body_t)
    df = pd.DataFrame(
        data={'Date_Sent(UTC)': date_list, 'Sender': from_list, 'Email': sender_email, 'Subject': subject_text,
              'Body': body_text})
    #logger.info("appended messages to dataframe")
    return df


def clean_up(dframe):
    x = r"\$\b(?<!/)(\d{1,5}(?:,\d{3})*(?:\.\d{2})?)\b(?!/)"
    dframe["Ms"] = dframe['Body'].str.split('\r\n\r\n').str[2]
    dframe['rep'] = dframe['Ms'].str.replace("=C2=A0", "")
    dframe['Message'] = dframe['rep'].str.split('Message:').str[1]
    dframe['Message']=dframe['Message'].str.replace("\r\n","")
    # df["Message"] = df.Ms.str[10:]
    dframe['Message'] = dframe['Message'].str.replace('3D', "")
    dframe['Message'] = dframe['Message'].fillna("Other Contributions")
    dframe['Amount'] = dframe['Body'].str.split('\r\n\r\n').str[1].str.replace('=\r\n', "").str.extract(x)
    dframe['Amount'] = dframe['Amount'].str.replace(',', "")
    dframe['Amount'] = pd.to_numeric(dframe['Amount'])
    dframe['date_utc'] = pd.to_datetime(dframe['Date_Sent(UTC)'])
    dframe['Date Sent'] = dframe['date_utc'].dt.tz_convert('US/Eastern')
    dframe['Sender'] = dframe['Sender'].str.split('<').str[0]
    dframe['Email'] = dframe['Email'].str.split('<').str[1]
    dframe['Email'] = dframe['Email'].str.replace(">", "")
    dframe = dframe[['Date Sent', 'Sender', 'Email', 'Amount', 'Message']]
    #logger.info("cleaned up columns")
    return dframe


def str_load(sav_file):
    # load yaml to get saved string
    # save string into a variable
    # return string

    with open(sav_file) as f:
        content = f.read()
        oldStr = yaml.load(content, Loader=yaml.FullLoader)
        latest_rw = oldStr["latest"]
    return latest_rw


def remove_dup(src_file, df_c):
    # put string together
    # read in saved string from str_load function
    # identify row that matches the string
    # delete all rows from that string and below
    # save latest string and pass string into str_dump function
    # delete column new
    # return new df
    df_f = df_c.copy()
    df_f['new'] = df_f['Date Sent'].astype(str) + df_f['Sender'] + df_f['Email']
    df_f['new'] = df_f['new'].str.replace(" ", "")
    df_f['Message'] = df_f['Message'].str.replace("=20", "")
    recent_str = str_load(src_file)
    #logger.info('loaded latest transaction from previous batch')
    row_del = df_f.index[df_f['new'] == recent_str].tolist()
    final_df = df_f.iloc[:row_del[0]]
    new_str = final_df['new'][0]
    str_dump(new_str, src_file)
    #logger.info('updated latest transaction')
    final_df = final_df.drop('new', axis=1)
   # logger.info('generated new data')
    return final_df


def str_dump(latest_str, dest_file):
    with open(dest_file) as f:
        new = yaml.safe_load(f)
        new['latest'] = latest_str
    with open(dest_file, 'w') as f:
        yaml.dump(new, f, sort_keys=False)


def send_email(send_to, subject, df_n):
    now = datetime.now()
    sub_string = now.strftime("%d/%m/%Y %H:%M:%S")
    send_from = "xxx@mail.com"
    password = "m____x"
    message = """\
    <p><strong>Please find attached the e-transfer transactions for the past 7 days including donation categories.&nbsp;</strong></p>
    <p><br></p>
    <p><strong>Regards&nbsp;</strong><br><strong>...&nbsp;    </strong></p>
    """

    multipart = MIMEMultipart()
    multipart["From"] = send_from
    multipart["To"] = send_to
    multipart["Subject"] = subject+sub_string
    attachment = MIMEApplication(df_n.to_csv())
    attachment["Content-Disposition"] = 'attachment; filename=" {}"'.format(f"{subject}.csv")
    multipart.attach(attachment)
    multipart.attach(MIMEText(message, "html"))
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(multipart["From"], password)
    server.sendmail(multipart["From"], multipart["To"], multipart.as_string())
    server.quit()
    #logger.info("email sent")


def main():
    msg = log_on_fetch("cred.yml")
    df_new = append_msgs(msg)
    logger=logging.getLogger()
    if logger.hasHandlers():
        logger.handlers=[]
    logger.propagate = False
    #server = smtplib.SMTP("smtp.gmail.com", 587)
    #smtp.starttls()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.handlers.SMTPHandler(mailhost=("smtp.gmail.com", 587),
                    fromaddr="xxx@mail.com",
                    toaddrs="xy@gmail.com",subject="EXCEPTION",
                    credentials=('xx@mail.com', 'm____x'),secure=()))
    try:
        df_clean = clean_up(df_new)
        df_final = remove_dup('lat.yml', df_clean)
        send_email("xy@gmail.com", "transactions-", df_final)
    except Exception as e:
        logger.exception(e)
    

main()
