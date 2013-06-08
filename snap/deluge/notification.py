#!/usr/bin/python

import smtplib
import os
import traceback
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email import Encoders
from snap.pyglog import *

gmail_user = None
gmail_pwd = None

def SendMailUnreliable(to, subject, text, attach):
  msg = MIMEMultipart()  
  msg['From'] = gmail_user
  msg['To'] = to
  msg['Subject'] = subject  
  msg.attach(MIMEText(text))  
  if attach:
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(open(attach, 'rb').read())
    Encoders.encode_base64(part)
    part.add_header('Content-Disposition',
            'attachment; filename="%s"' % os.path.basename(attach))
    msg.attach(part)  
  mailServer = smtplib.SMTP("smtp.gmail.com", 587)
  mailServer.ehlo()
  mailServer.starttls()
  mailServer.ehlo()
  mailServer.login(gmail_user, gmail_pwd)
  mailServer.sendmail(gmail_user, to, msg.as_string())
  # Should be mailServer.quit(), but that crashes...
  mailServer.close()
  return

def SendMail(to, subject, text, attach):
  for i in range(2):
    try:
      SendMailUnreliable(to, subject, text, attach)
      break
    except:      
      LOG(INFO, 'failed to send notification email... will try again...')
      print traceback.format_exc()            
    time.sleep(5)
  return

def SendNotification(recipient_email, title, message, attachment = None):
  subject_prefix = '[deluge]'
  subject = subject_prefix + ' ' + title  
  SendMail(recipient, subject, message, attachment )
  return


    
  
