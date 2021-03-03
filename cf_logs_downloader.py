#!/usr/bin/env python3

#import libraries needed in this program
#'requests' library needs to be installed first
import requests, time, threading, os, json, logging, sys, argparse, logging.handlers
from datetime import datetime, date, timedelta
from pathlib import Path
from requests.packages.urllib3.exceptions import InsecureRequestWarning

#specify version number of the program
ver_num = "2.00"

#a flag to determine whether the user wants to exit the program, so can handle the program exit gracefully
is_exit = False

#determine how many logpull process are running
num_of_running_thread = 0

#define the timestamp format that we supply to Cloudflare API
timestamp_format = "rfc3339"

#the prefix name of the logfile name
logfile_name_prefix = "cf_logs"

#initialize the variables to empty string, so the other parts of the program can access it
path = zone_id = access_token = sample_rate = port = start_time = end_time = ""

#the default value for the interval between each logpull process
interval = 60.0

#specify the number of attempts to retry in the event of error
retry_attempt = 5

#disable unverified HTTPS request warning in when using Requests library
#requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

#by default, 
#raw logs will be stored on local storage
#logpull operation will be repeated unless user specifies to do one-time operation
one_time = no_organize = no_gzip = False

'''
Specify the fields for the logs

The following fields are available: BotScore,BotScoreSrc,CacheCacheStatus,CacheResponseBytes,CacheResponseStatus,CacheTieredFill,ClientASN,ClientCountry,ClientDeviceType,ClientIP,ClientIPClass,ClientRequestBytes,ClientRequestHost,ClientRequestMethod,ClientRequestPath,ClientRequestProtocol,ClientRequestReferer,ClientRequestURI,ClientRequestUserAgent,ClientSSLCipher,ClientSSLProtocol,ClientSrcPort,ClientXRequestedWith,EdgeColoCode,EdgeColoID,EdgeEndTimestamp,EdgePathingOp,EdgePathingSrc,EdgePathingStatus,EdgeRateLimitAction,EdgeRateLimitID,EdgeRequestHost,EdgeResponseBytes,EdgeResponseCompressionRatio,EdgeResponseContentType,EdgeResponseStatus,EdgeServerIP,EdgeStartTimestamp,FirewallMatchesActions,FirewallMatchesRuleIDs,FirewallMatchesSources,OriginIP,OriginResponseBytes,OriginResponseHTTPExpires,OriginResponseHTTPLastModified,OriginResponseStatus,OriginResponseTime,OriginSSLProtocol,ParentRayID,RayID,RequestHeaders,SecurityLevel,WAFAction,WAFProfile,WAFRuleID,WAFRuleMessage,WorkerCPUTime,WorkerStatus,WorkerSubrequest,WorkerSubrequestCount,ZoneID

Deprecated log fields: WAFFlags,WAFMatchedVar
'''
fields = "BotScore,BotScoreSrc,CacheCacheStatus,CacheResponseBytes,CacheResponseStatus,CacheTieredFill,ClientASN,ClientCountry,ClientDeviceType,ClientIP,ClientIPClass,ClientRequestBytes,ClientRequestHost,ClientRequestMethod,ClientRequestPath,ClientRequestProtocol,ClientRequestReferer,ClientRequestURI,ClientRequestUserAgent,ClientSSLCipher,ClientSSLProtocol,ClientSrcPort,ClientXRequestedWith,EdgeColoCode,EdgeColoID,EdgeEndTimestamp,EdgePathingOp,EdgePathingSrc,EdgePathingStatus,EdgeRateLimitAction,EdgeRateLimitID,EdgeRequestHost,EdgeResponseBytes,EdgeResponseCompressionRatio,EdgeResponseContentType,EdgeResponseStatus,EdgeServerIP,EdgeStartTimestamp,FirewallMatchesActions,FirewallMatchesRuleIDs,FirewallMatchesSources,OriginIP,OriginResponseBytes,OriginResponseHTTPExpires,OriginResponseHTTPLastModified,OriginResponseStatus,OriginResponseTime,OriginSSLProtocol,ParentRayID,RayID,RequestHeaders,SecurityLevel,WAFAction,WAFProfile,WAFRuleID,WAFRuleMessage,WorkerCPUTime,WorkerStatus,WorkerSubrequest,WorkerSubrequestCount,ZoneID"

#create three logging object for logging purposes
logger = logging.getLogger("general_logger") #for general logging
succ_logger = logging.getLogger("succ_logger") #to log successful attempts
fail_logger = logging.getLogger("fail_logger") #to log failed attempts

#the default logging level is INFO, which is one level higher than DEBUG
logger.setLevel(logging.INFO)
succ_logger.setLevel(logging.INFO)
fail_logger.setLevel(logging.INFO)

#create handlers to write logs to local storage, and automatically rotate them
Path("/var/log/cf_logs_downloader/").mkdir(parents=True, exist_ok=True)
handler_file = logging.handlers.TimedRotatingFileHandler("/var/log/cf_logs_downloader/pull.log", when='H', interval=1, backupCount=120, utc=False, encoding="utf-8") #rotate hourly, store up to 120 hours
succ_handler_file = logging.handlers.TimedRotatingFileHandler("/var/log/cf_logs_downloader/succ.log", when='D', interval=1, backupCount=30, utc=False, encoding="utf-8") #rotate daily, store up to 30 days
fail_handler_file = logging.handlers.TimedRotatingFileHandler("/var/log/cf_logs_downloader/fail.log", when='D', interval=1, backupCount=30, utc=False, encoding="utf-8") #rotate daily, store up to 30 days

#create a handler to print logs on terminal
handler_console = logging.StreamHandler()

#define the format of the logs for any logging event occurs
formatter = logging.Formatter("[%(levelname)s] %(message)s") #print log level with message
succfail_formatter = logging.Formatter("%(message)s") #print message only

#set the log format for all the handlers
handler_file.setFormatter(formatter)
handler_console.setFormatter(formatter)
succ_handler_file.setFormatter(succfail_formatter)
fail_handler_file.setFormatter(succfail_formatter)

#finally, add all handlers to their respective loggers
logger.addHandler(handler_file)
logger.addHandler(handler_console)
succ_logger.addHandler(succ_handler_file)
fail_logger.addHandler(fail_handler_file)

'''
This is the starting point of the program. It will initialize the parameters supplied by the user and save it in a variable.
Help(welcome) message will be displayed if the user specifies -h or --help as the parameter.
If required parameters are not given by the user, an error message will be displayed to the user and the program will exit.
'''
def initialize_arg():
    
    global path, zone_id, access_token, sample_rate, interval, logger, logfile_name_prefix, start_time_static, end_time_static, one_time, no_organize, no_gzip
    
    welcome_msg = "A little tool to pull/download HTTP Access logs from Cloudflare Enterprise Log Share (ELS) and save it on local storage."

    #create an argparse object with the welcome message as the description
    parser = argparse.ArgumentParser(description=welcome_msg)
    
    #specify which arguments are available to use in this program. The usage of the arguments will be printed when the user tells the program to display help message.
    parser.add_argument("-z", "--zone", help="Specify the Cloudflare Zone ID, if CF_ZONE_ID environment variable not set. This will override CF_ZONE_ID variable.")
    parser.add_argument("-t", "--token", help="Specify your Cloudflare Access Token, if CF_TOKEN environment variable not set. This will override CF_TOKEN variable.")
    parser.add_argument("-r", "--rate", help="Specify the log sampling rate from 0.01 to 1. Default is 1.", default="1")
    parser.add_argument("-i", "--interval", help="Specify the interval between each logpull in seconds. Default is 60 seconds.", default=60.0, type=float)
    parser.add_argument("--path", help="Specify the path to store logs. By default, it will save to /var/log/cf_logs/", default="/var/log/cf_logs/")
    parser.add_argument("--prefix", help="Specify the prefix name of the logfile being stored on local storage. By default, the file name will begins with cf_logs.", default="cf_logs")
    parser.add_argument("--no-organize", help="Instruct the program to store raw logs as is, without organizing them into date and time folder.", action="store_true")
    parser.add_argument("--no-gzip", help="Do not compress the raw logs.", action="store_true")
    parser.add_argument("--one-time", help="Only pull logs from Cloudflare for one time, without scheduling capability. You must specify the start time and end time of the logs to be pulled from Cloudflare.", action="store_true")
    parser.add_argument("--start-time", help="Specify the start time of the logs to be pulled from Cloudflare. The start time is inclusive. You must follow the ISO 8601 date format, in UTC timezone. Example: 2020-12-31T12:34:56Z")
    parser.add_argument("--end-time", help="Specify the end time of the logs to be pulled from Cloudflare. The end time is exclusive. You must follow the ISO 8601 date format, in UTC timezone. Example: 2020-12-31T12:35:00Z")
    parser.add_argument("--debug", help="Enable debugging functionality.", action="store_true")
    parser.add_argument("-v", "--version", help="Show program version.", action="version", version="Version " + ver_num)
    
    #parse the parameters supplied by the user, and check whether the parameters match the one specified above
    #if it does not match, an error message will be given to the user and the program will exit
    args = parser.parse_args()
    
    #enable debugging if specified by the user
    if args.debug is True:
        logger.setLevel(logging.DEBUG)
    
    #take the "path" parameter given by the user and assign it to a variable
    path = args.path
    
    #check whether Zone ID is given by the user via the parameter. If not, check the environment variable.
    #the Zone ID given via the parameter will override the Zone ID inside environment variable.
    #if no Zone ID is given, an error message will be given to the user and the program will exit
    if args.zone:
        zone_id = args.zone
    elif os.getenv("CF_ZONE_ID"):
        zone_id = os.getenv("CF_ZONE_ID")
    else:
        logger.critical(str(datetime.now()) + " --- Please specify your Cloudflare Zone ID.")
        sys.exit(2)
        
    #check whether Cloudflare Access Token is given by the user via the parameter. If not, check the environment variable.
    #the Cloudflare Access Token given via the parameter will override the Cloudflare Access Token inside environment variable.
    #if no Cloudflare Access Token is given, an error message will be given to the user and the program will exit
    if args.token:
        access_token = args.token
    elif os.getenv("CF_TOKEN"):
        access_token = os.getenv("CF_TOKEN")
    else:
        logger.critical(str(datetime.now()) + " --- Please specify your Cloudflare Access Token.")
        sys.exit(2)
    
    #check whether the sample rate is valid, if not return an error message and exit
    try:
        #the value should not more than two decimal places
        if len(args.rate.split(".", 1)[1]) > 2:
            logger.critical(str(datetime.now()) + " --- Invalid sample rate specified. Please specify a value between 0.01 and 1, and only two decimal places allowed.")
            sys.exit(2)
    except IndexError:
        #sometimes the user may specify 1 as the value, so we need to handle the exception for value with no decimal places
        pass
    if float(args.rate) <= 1.0 and float(args.rate) >= 0.01:
        sample_rate = args.rate
    else:
        logger.critical(str(datetime.now()) + " --- Invalid sample rate specified. Please specify a value between 0.01 and 1, and only two decimal places allowed.")
        sys.exit(2)
    
    one_time = args.one_time
    if one_time is True:
        if args.start_time and args.end_time:
            try:
                start_time_static = datetime.strptime(args.start_time, "%Y-%m-%dT%H:%M:%SZ")
                end_time_static = datetime.strptime(args.end_time, "%Y-%m-%dT%H:%M:%SZ")
                diff_start_end = end_time_static - start_time_static
                diff_to_now = datetime.utcnow() - end_time_static
                if diff_start_end.total_seconds() < 1:
                    logger.critical(str(datetime.now()) + " --- Start time must be earlier than the end time by at least 1 second. ")
                    sys.exit(2)
                if diff_to_now.total_seconds() < 70:
                    logger.critical(str(datetime.now()) + " --- Please specify an end time that is 70 seconds or more earlier than the current time.")
                    sys.exit(2)
            except ValueError:
                logger.critical(str(datetime.now()) + " --- Invalid date format specified. Make sure it is in ISO 8601 date format, in UTC timezone. Please refer to the example: 2020-12-31T12:34:56Z")
                sys.exit(2)
        else:
            logger.critical(str(datetime.now()) + " --- No start time or end time specified for one-time operation. ")
            sys.exit(2)
    
    #take the interval, logfile name prefix and pipeline setting parameter given by the user and assign it to a variable
    interval = args.interval
    logfile_name_prefix = args.prefix
    no_organize = args.no_organize
    no_gzip = args.no_gzip
    
    
'''
This method will be invoked after initialize_arg().
This method is to verify whether the Cloudflare Zone ID and Cloudflare Access Token given by the user is valid.
If it is not valid, an error message will be given to the user and the program will exit
'''
def verify_credential():
    
    global logger
    
    #specify the Cloudflare API URL to check the Zone ID and Access Token
    url = "https://api.cloudflare.com/client/v4/zones/" + zone_id + "/logs/received"
    headers = {"Authorization": "Bearer " + access_token, "Content-Type": "application/json"}
    
    #make a HTTP request to the Cloudflare API
    r = requests.get(url, headers=headers)
    r.encoding = "utf-8"
    
    #if there's an error, Cloudflare API will return a JSON object to indicate the error
    #and if it's not, a plain text will be returned instead
    #the try except block is to catch any errors raised by json.loads(), in case Cloudflare is not returning JSON object
    try:
        response = json.loads(r.text)
        if response["success"] is False:
            logger.critical(str(datetime.now()) + " --- Failed to authenticate with Cloudflare API. Please check your Zone ID and Cloudflare Access Token.")
            sys.exit(2)
    except json.JSONDecodeError:
        #a non-JSON object returned by Cloudflare indicates that authentication successful
        pass
    

'''
This method is to initialize the folder with the date and time of the logs being stored on local storage as the name of the folder
If the folder does not exist, it will automatically create a new one
'''
def initialize_folder(path_with_date):
    data_folder = Path(path_with_date)
    data_folder.mkdir(parents=True, exist_ok=True)
    return data_folder

'''
This method is to prepare the path of where the logfile will be stored and what will be the name of the logfile.
If the logfile already exists, we assume that the logs has been pulled from Cloudflare previously
'''
def prepare_path(log_start_time_rfc3339, log_end_time_rfc3339, data_folder):
    logfile_name = logfile_name_prefix + "_" + log_start_time_rfc3339 + "~" + log_end_time_rfc3339 + ".json"
    logfile_path = data_folder / logfile_name
    
    if os.path.exists(str(logfile_path) + ".gz") or os.path.exists(str(logfile_path) + ".json"):
        return False
    else:
        return logfile_path
    
'''
A method to check whether the user initiates program exit.
This method will be triggered every time the logpull thread finishes its job (which is, finish the logpull)
This method will minus 1 from the total number of running threads, and check whether the user triggers the program exit process.
If program exit initiated by user, is_exit will become True, and this method will make sure that number of running threads must be zero in order to exit the program gracefully.
'''
def check_if_exited():
    global is_exit, num_of_running_thread
    
    num_of_running_thread -= 1

    if is_exit is True and num_of_running_thread <= 0:
        logger.info(str(datetime.now()) + " --- Program exited gracefully.")
        return True
    
    return False

'''
A method that is responsible for just compressing logs that is written to the local storage, in gzip format
'''
def compress_logs(logfile_path):
    exit_code = os.system("gzip -f " + str(logfile_path))
    logger.debug(str(datetime.now()) + " --- Gzip executed with exit code: " + str(exit_code))
    
    return True if exit_code == 0 else False

'''
This method is responsible to write logs to local storage after the logs have been pulled from Cloudflare API.
After successfully save the logs, it will also trigger compress_logs() method to compress the newly written logs.
'''
def write_logs(log_start_time_rfc3339,  log_end_time_rfc3339, logfile_path, data):
    
    #open the file as write mode
    try:
        logfile = open(logfile_path, mode="w", encoding="utf-8")
        logfile.write(data)
        logfile.close()
    except:
        return False
    
    return True

          
'''
This method will handle the overall log processing tasks and it will run as a separate thread.
Based on the interval setting configured by the user, this method will only handle logs for a specific time slot.
'''
def logs(current_time, log_start_time_utc, log_end_time_utc):
    
    global path, num_of_running_thread, logger, retry_attempt, no_organize, no_gzip
    
    #add one to the variable to indicate number of running threads. useful to determine whether to exit the program gracefully
    num_of_running_thread += 1
    
    #a variable to check whether the request to Cloudflare API is successful.
    request_success = False
    
    #if the user instructs the program to do logpull for only one time, the logs will not be stored in folder that follows the naming convention: date and time
    if one_time is True or no_organize is True:
        pass
    else:
        #get the current date and hour, these will be used to initialize the folder to store the logs
        today_date = str(current_time.date())
        current_hour = str(current_time.hour) + "00"
    
    #get the log start time and log end time in RFC3339 format, so Cloudflare API will understand it and pull the appropriate logs for us
    log_start_time_rfc3339 = log_start_time_utc.isoformat() + 'Z'
    log_end_time_rfc3339 = log_end_time_utc.isoformat() + 'Z'
        
    #initialize the folder with the path specified below
    #if the user instructs the program to do logpull for only one time, it will be stored in another folder instead of the naming convention of the folder: date and time
    if one_time is True or no_organize is True:
        path_with_date = path
    else:
        path_with_date = path + ("/" + today_date + "/" + current_hour)
    data_folder = initialize_folder(path_with_date)

    #prepare the full path (incl. file name) to store the logs
    logfile_path = prepare_path(log_start_time_rfc3339, log_end_time_rfc3339, data_folder)

    #check the returned value from prepare_path() method. if False, means logfile already exists and no further action required
    if logfile_path is False:

        logger.warning(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Logfile already exists! Skipping.")

        return check_if_exited()
    
    #specify the URL for the Cloudflare API endpoint, with parameters such as Zone ID, the start time and end time of the logs to pull, timestamp format, sample rate and the fields to be included in the logs
    url = "https://api.cloudflare.com/client/v4/zones/" + zone_id + "/logs/received?start=" + log_start_time_rfc3339 + "&end=" + log_end_time_rfc3339 + "&timestamps="+ timestamp_format +"&sample=" + sample_rate + "&fields=" + fields

    #specify headers for the content type and access token 
    headers = {"Authorization": "Bearer " + access_token, "Content-Type": "application/json"}

    logger.info(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Requesting logs from Cloudflare...")
    
    #5 retries will be given for the logpull process, in case something happens
    for i in range(retry_attempt+1):
        #make a GET request to the Cloudflare API
        r = requests.get(url, headers=headers)
        r.encoding = 'utf-8'
        
        #check whether the HTTP response code is 200, if yes then logpull success and exit the loop
        if r.status_code == 200:
            request_success = True
            break
        else:
            #if HTTP response code is not 200, means something happened
            logger.debug(str(datetime.now()) + " --- Output from Cloudflare API:\n" + r.text) #the raw response will be logged only if the user enables debugging
            try:
                #load the JSON object to better access the content of it
                response = json.loads(r.text)
            except:
                #something weird happened if the response is not a JSON object, thus print out the error dump
                logger.error(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Unknown error occured with error code " + str(r.status_code) + ". Error dump: " + r.text + ". " + (("Retrying " + str(i+1) + " of " + str(retry_attempt) + "...") if i < (retry_attempt) else ""))
                time.sleep(3)
                continue

            #to check whether "success" key exists in JSON object, if yes, check whether the value is False, and print out the error message
            if "success" in response:
                if response["success"] is False:
                    logger.error(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Failed to request logs from Cloudflare with error code " + str(response["errors"][0]["code"]) + ": " + response["errors"][0]["message"] + ". " + (("Retrying " + str(i+1) + " of " + str(retry_attempt) + "...") if i < (retry_attempt) else ""))
                    time.sleep(3)
                    continue
                else:
                    #something weird happened if it is not False. If the request has been successfully done, it should not return this kind of error, instead the raw logs should be returned with HTTP response code 200.
                    logger.error(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Unknown error occured with error code " + str(r.status_code) + ". Error dump: " + r.text + ". " + (("Retrying " + str(i+1) + " of " + str(retry_attempt) + "...") if i < (retry_attempt) else ""))
                    time.sleep(3)
                    continue
            else:
                #other type of error may occur, which may not return a JSON object.
                logger.error(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Unknown error occured with error code " + str(r.status_code) + ". Error dump: " + r.text + ". " + (("Retrying " + str(i+1) + " of " + str(retry_attempt) + "...") if i < (retry_attempt) else ""))
                time.sleep(3)
                continue
            
    #check whether the logpull process from Cloudflare API has been successfully completed, if yes then proceed with next steps
    if request_success is False:
        fail_logger.error("Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + " (Logpull error)")
        return check_if_exited()

    #Proceed to save the logs
    logger.info(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Logs requested. Saving logs...")
    if write_logs(log_start_time_rfc3339,  log_end_time_rfc3339, logfile_path, r.text):
        #successful of write logs
        logger.info(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Logs saved as " + str(logfile_path) + ". " + ("Logs will not compressed." if no_gzip is True else ""))
    else:
        #unsuccessful of write logs
        logger.error(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Failed to save logs to local storage.")
        fail_logger.error("Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + " (Write log error)")
        return check_if_exited()

    if no_gzip is False:
        if compress_logs(logfile_path):
            #successful of compress logs
            logger.info(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Logs compressed in gzip format: " + str(logfile_path) + ".gz")
        else:
            #unsuccessful of compress logs
            logger.error(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": An error occured while compressing " + str(logfile_path) + ".gz")
            fail_logger.error("Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + " (Compress log error)")
            return check_if_exited()

    succ_logger.info("Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339)

    #invoke this method to check whether the user triggers program exit sequence
    return check_if_exited()

        
####################################################################################################       
        
        
#This is where the real execution of the program begins. First it will initialize the parameters supplied by the user
initialize_arg()

#After the above execution, it will verify the Zone ID and Access Token given by the user whether they are valid
verify_credential()

#if both Zone ID and Access Token are valid, the logpull tasks to Elastic will begin.
logger.info(str(datetime.now()) + " --- Cloudflare ELS logs download tasks started.")

#if the user instructs the program to do logpull for only one time, the program will not do the logpull jobs repeatedly
if one_time is True:
    threading.Thread(target=logs, args=(None, start_time_static, end_time_static)).start()
else:
    #first get the current system time, both local and UTC time.
    #the purpose of getting UTC time is to facilitate the calculation of the start and end time to pull the logs from Cloudflare API
    #the purpose of getting local time is to generate a directory structure to store logs, separated by the date and time
    current_time_utc = datetime.utcnow()
    current_time = datetime.now()

    #calculate how many seconds to go back from current time to pull the logs. 
    logs_from = 60.0 + ((interval // 60 * 60) + 60)

    #calculate the start time to pull the logs from Cloudflare API
    log_start_time_utc = current_time_utc.replace(second=0, microsecond=0) - timedelta(seconds=logs_from)
    current_time = current_time.replace(second=0, microsecond=0) - timedelta(seconds=logs_from)

    #this is useful when we need to repeat the execution of a code block after a certain interval, in an accurate way
    #below code will explain the usage of this in detail
    initial_time = time.time()

    #force the program to run indefinitely, unless the user stops it with Ctrl+C
    while True:

        #calculate the end time to pull the logs from Cloudflare API, based on the interval value given by the user
        log_end_time_utc = log_start_time_utc + timedelta(seconds=interval)

        #create a new thread to handle the logs processing. the target method is logs() and 3 parameters are supplied to this method
        threading.Thread(target=logs, args=(current_time, log_start_time_utc, log_end_time_utc)).start()

        #assigning start and end time to the next iteration
        log_start_time_utc = log_end_time_utc
        current_time = current_time + timedelta(seconds=interval)

        try:
            time.sleep(interval - ((time.time() - initial_time) % interval))
        except KeyboardInterrupt:
            is_exit = True
            print("")
            logger.info(str(datetime.now()) + " --- Initiating program exit. Finishing up log download tasks...")
            if num_of_running_thread <= 0:
                logger.info(str(datetime.now()) + " --- Program exited gracefully.")
            break
        
