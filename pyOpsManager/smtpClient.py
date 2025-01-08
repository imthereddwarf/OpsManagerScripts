'''
Created on Feb 3, 2022

@author: peter.williamson
'''
import smtplib,ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class emailError(Exception):
    pass

class fatalError(emailError):
    # We can't proceed due to an error
    pass

class smtpClient:
    '''
    classdocs
    '''
    PREAMBLE = "<!DOCTYPE html><html><style>table, th, td {border:1px solid black;border-collapse: collapse;}td{text-align: center; vertical-align: middle;background-color: #F8F3F3}th {background-color: #CD1409;color: white;}</style><body>Dear MongoDB Team,<br></br>"
    POSTAMBLE = "<importantLink><br><br><b>NOTE: </b><i>This message may contain confidential and/or privileged information. If you are not the addressee or authorized to receive this for the addressee, you must not use, copy, disclose, or take any action based on this message or any information herein. If you have received this message in error, please advise the sender immediately by reply email and delete this message. Thank you for your cooperation.</i></br></br></importantLink><br><br>Thanks,<pre>The Ops Manager Team</pre></br></html>"
    START_TABLE = "<table style='width:70%'>"
    END_TABLE = "<table style='width:70%'>"
    HEADING = "<h4>{}</h4>"
    
    def __init__(self, server, port=25, user=None,password=None,sender=None,useSSL=False):
        '''
        Constructor
        '''
        self.smtp_server = server
        self.port = port
        self.ssl = useSSL

        if (user != None):
            if (password == None):
                raise fatalError("Password is required if user is specified.")
            else:
                self.dologin = True
                self.user = user
                self.password = password
        else:
            self.dologin = False
            self.user = None
            self.password = None
        if useSSL:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.smtp_server,self.port,context=context) as server:
                if self.dologin:
                    server.login(self.user,self.password)
                    server.noop()
        else:
            with smtplib.SMTP(self.smtp_server,self.port) as server:
                server.noop()
        self.sender = sender
        
    def sendMessage(self,recipient,subject,body):
        message = "Subject: "+subject+"\n\n"+body
        if self.ssl:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.smtp_server,self.port,context=context) as server:
                if self.dologin:
                    server.login(self.user,self.password)
                    server.sendmail(self.sender,recipient,message)
        else:
            with smtplib.SMTP(self.smtp_server,self.port) as server:
                server.sendmail(self.sender,recipient,message)
                
    def sendHTML(self,recipient,subject,body,plaintext=None):
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = self.sender
        message["To"] = recipient
        
        if plaintext == None:
            part1 = MIMEText("This is an HTML message","plain")
        else:
            part1 = MIMEText(plaintext)
        part2 = MIMEText(body,"html")
        message.attach(part1)
        message.attach(part2)
        if self.ssl:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.smtp_server,self.port,context=context) as server:
                if self.dologin:
                    server.login(self.user,self.password)
                    server.sendmail(self.sender,recipient,message)
        else:
            with smtplib.SMTP(self.smtp_server,self.port) as server:
                server.sendmail(self.sender,recipient,message.as_string())
                
    def sendHTMLTABLE(self,recipient,subject,title,data):
        body = self.PREAMBLE
        body += self.HEADING.format(title)
        body += self.START_TABLE
        columns = []
        if ("colHeadings" in data) and isinstance(data["colHeadings"],list):
            body += "<tr>"
            for colName in data["colHeadings"]:
                columns.append(colName)
                body+= "<th>"+colName+"</th>"
            body += "</tr>"
        if ("tableRows" in data) and isinstance(data["tableRows"],list):
            for row in data["tableRows"]:
                # Each data row is a dict so observe the field names
                if isinstance(row,dict):
                    if len(columns) == 0:  # Use fieldnames from the first row
                        body += "<tr>"
                        for key in row:
                            columns.append(key)
                            body += "<th>"+key+"</th>"
                        body += "</tr>"
                    body += "<tr>"
                    for key in columns:
                        if key in row:
                            body += "<td>"+str(row[key])+"</td>"
                        else:
                            body += "<td></td>"
                    body += "</tr>"
                # If each row is a nested array include in order
                elif isinstance(row,list):
                    body += "<tr>"
                    for key in row:
                        body += "<td>"+str(key)+"</td>"
                    body += "</tr>"
        body += self.END_TABLE
        body += self.POSTAMBLE
        
        self.sendHTML(recipient,subject,body)
                
                    
            
                
        
        
        