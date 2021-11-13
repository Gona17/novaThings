from google.oauth2 import service_account
import googleapiclient.discovery


SCOPES = ['https://www.googleapis.com/oauth2/v1/certs']
SERVICE_ACCOUNT_FILE = 'E:/Escritorio/Nova/GoogleAnalytics/resources/client_secrets.json'

credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

sqladmin = googleapiclient.discovery.build('analyticsreporting', 'v4', credentials=credentials)



