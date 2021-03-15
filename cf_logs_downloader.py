#!/usr/bin/env python3

#import libraries needed in this program
#'requests' library needs to be installed first
import requests, time, threading, os, json, logging, sys, argparse, logging.handlers, yaml, yschema
from datetime import datetime, date, timedelta
from pathlib import Path
from requests.packages.urllib3.exceptions import InsecureRequestWarning

#specify version number of the program
ver_num = "2.1.1"

#a flag to determine whether the user wants to exit the program, so can handle the program exit gracefully
is_exit = False

#determine how many logpull process are running
num_of_running_thread = 0

#define the timestamp format that we supply to Cloudflare API
timestamp_format = "rfc3339"

#the prefix name of the logfile name
logfile_name_prefix = "cf_logs"

#initialize the variables to empty string, so the other parts of the program can access it
path = zone_id = access_token = sample_rate = port = start_time = end_time = final_fields = ""

#the default value for the interval between each logpull process
interval = 60.0

#specify the number of attempts to retry in the event of error
retry_attempt = 5

#disable unverified HTTPS request warning in when using Requests library
#requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

#by default, 
#raw logs will be stored on local storage
#logpull operation will be repeated unless user specifies to do one-time operation
one_time = no_organize = no_gzip = bot_management = False

#specify the schema for the YAML configuration file
yaml_schema = {'optional cf_zone_id': 'str', 'optional cf_token': 'str', 'optional rate': 'float', 'optional interval': 'int', 'optional path': 'str', 'optional prefix': 'str', 'optional no_organize': 'bool', 'optional no_gzip': 'bool', 'optional bot_management': 'bool', 'optional debug': 'bool'}

'''
Specify the fields for the logs

The following fields are available: BotScore,BotScoreSrc,CacheCacheStatus,CacheResponseBytes,CacheResponseStatus,CacheTieredFill,ClientASN,ClientCountry,ClientDeviceType,ClientIP,ClientIPClass,ClientRequestBytes,ClientRequestHost,ClientRequestMethod,ClientRequestPath,ClientRequestProtocol,ClientRequestReferer,ClientRequestURI,ClientRequestUserAgent,ClientSSLCipher,ClientSSLProtocol,ClientSrcPort,ClientXRequestedWith,EdgeColoCode,EdgeColoID,EdgeEndTimestamp,EdgePathingOp,EdgePathingSrc,EdgePathingStatus,EdgeRateLimitAction,EdgeRateLimitID,EdgeRequestHost,EdgeResponseBytes,EdgeResponseCompressionRatio,EdgeResponseContentType,EdgeResponseStatus,EdgeServerIP,EdgeStartTimestamp,FirewallMatchesActions,FirewallMatchesRuleIDs,FirewallMatchesSources,OriginIP,OriginResponseBytes,OriginResponseHTTPExpires,OriginResponseHTTPLastModified,OriginResponseStatus,OriginResponseTime,OriginSSLProtocol,ParentRayID,RayID,RequestHeaders,SecurityLevel,WAFAction,WAFProfile,WAFRuleID,WAFRuleMessage,WorkerCPUTime,WorkerStatus,WorkerSubrequest,WorkerSubrequestCount,ZoneID

Deprecated log fields: WAFFlags,WAFMatchedVar
'''
general_fields = ["CacheCacheStatus","CacheResponseBytes","CacheResponseStatus","CacheTieredFill","ClientASN","ClientCountry","ClientDeviceType","ClientIP","ClientIPClass","ClientRequestBytes","ClientRequestHost","ClientRequestMethod","ClientRequestPath","ClientRequestProtocol","ClientRequestReferer","ClientRequestURI","ClientRequestUserAgent","ClientSSLCipher","ClientSSLProtocol","ClientSrcPort","ClientXRequestedWith","EdgeColoCode","EdgeColoID","EdgeEndTimestamp","EdgePathingOp","EdgePathingSrc","EdgePathingStatus","EdgeRateLimitAction","EdgeRateLimitID","EdgeRequestHost","EdgeResponseBytes","EdgeResponseCompressionRatio","EdgeResponseContentType","EdgeResponseStatus","EdgeServerIP","EdgeStartTimestamp","FirewallMatchesActions","FirewallMatchesRuleIDs","FirewallMatchesSources","OriginIP","OriginResponseBytes","OriginResponseHTTPExpires","OriginResponseHTTPLastModified","OriginResponseStatus","OriginResponseTime","OriginSSLProtocol","ParentRayID","RayID","RequestHeaders","SecurityLevel","WAFAction","WAFProfile","WAFRuleID","WAFRuleMessage","WorkerCPUTime","WorkerStatus","WorkerSubrequest","WorkerSubrequestCount","ZoneID"]

bot_fields = ["BotScore","BotScoreSrc"]

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
    
    global path, zone_id, access_token, sample_rate, interval, logger, logfile_name_prefix, start_time_static, end_time_static, one_time, no_organize, no_gzip, bot_management, general_fields, final_fields, yaml_schema
    
    welcome_msg = "A little tool to pull/download HTTP Access logs from Cloudflare Enterprise Log Share (ELS) and save it on local storage."

    parsed_config = {}

    #create an argparse object with the welcome message as the description
    parser = argparse.ArgumentParser(description=welcome_msg)
    
    #specify which arguments are available to use in this program. The usage of the arguments will be printed when the user tells the program to display help message.
    parser.add_argument("-c", "--config", help="Specify the path to the YAML configuration file.")
    parser.add_argument("-z", "--zone", help="Specify the Cloudflare Zone ID, if CF_ZONE_ID environment variable not set. This will override CF_ZONE_ID variable.")
    parser.add_argument("-t", "--token", help="Specify your Cloudflare Access Token, if CF_TOKEN environment variable not set. This will override CF_TOKEN variable.")
    parser.add_argument("-r", "--rate", help="Specify the log sampling rate from 0.01 to 1. Default is 1.", type=float)
    parser.add_argument("-i", "--interval", help="Specify the interval between each logpull in seconds. Default is 60 seconds.", type=int)
    parser.add_argument("--path", help="Specify the path to store logs. By default, it will save to /var/log/cf_logs/")
    parser.add_argument("--prefix", help="Specify the prefix name of the logfile being stored on local storage. By default, the file name will begins with cf_logs.")
    parser.add_argument("--no-organize", help="Instruct the program to store raw logs as is, without organizing them into date and time folder.", action="store_true")
    parser.add_argument("--no-gzip", help="Do not compress the raw logs.", action="store_true")
    parser.add_argument("--bot-management", help="Specify this parameter if your zone has Bot Management enabled and you want to include Bot Management related fields in your logs.", action="store_true")
    parser.add_argument("--one-time", help="Only pull logs from Cloudflare for one time, without scheduling capability. You must specify the start time and end time of the logs to be pulled from Cloudflare.", action="store_true")
    parser.add_argument("--start-time", help="Specify the start time of the logs to be pulled from Cloudflare. The start time is inclusive. You must follow the ISO 8601 date format, in UTC timezone. Example: 2020-12-31T12:34:56Z")
    parser.add_argument("--end-time", help="Specify the end time of the logs to be pulled from Cloudflare. The end time is exclusive. You must follow the ISO 8601 date format, in UTC timezone. Example: 2020-12-31T12:35:00Z")
    parser.add_argument("--debug", help="Enable debugging functionality.", action="store_true")
    parser.add_argument("-v", "--version", help="Show program version.", action="version", version="Version " + ver_num)
    
    #parse the parameters supplied by the user, and check whether the parameters match the one specified above
    #if it does not match, an error message will be given to the user and the program will exit
    args = parser.parse_args()
        
    #check if user specifies the path to configuration file, if yes, attempt read settings from the configuration file
    if args.config:
        #check if configuration file exists. if not, display an error and exit.
        try:
            config_file = open(args.config, mode="r", encoding="utf-8")
        except Exception as e:
            logger.critical(str(datetime.now()) + " --- Error while opening " + args.config + ": " + e + ".")
            sys.exit(2)

        #if able to open the configuration file, load and parse the YAML data into Python dictionary.
        #if unable to parse the YAML data, display an error and exit.
        try:
            parsed_config = yaml.safe_load(config_file)
        except Exception as e:
            logger.critical(str(datetime.now()) + " --- Error parsing configuration file: " + (e))
            sys.exit(2)

        #check if the configuration follows the schema. If not, display an error and exit.
        try:
            yschema.validate(parsed_config, yaml_schema)
        except yschema.exceptions.ValidationError as e:
            logger.critical(str(datetime.now()) + " --- Error in configuration file: " + str(e) + ". Please check whether the settings are correct.")
            sys.exit(2)

    #enable debugging if specified by the user
    if args.debug is True or parsed_config.get("debug") is True:
        logger.setLevel(logging.DEBUG)

    #check whether path is given by the user via the parameter. If not, check the config file.
    #the path given via the parameter will override the path value inside config file.
    #if no path value specified, default value will be used.
    if args.path:
        path = args.path
    elif parsed_config.get("path"):
        path = parsed_config.get("path")
    else:
        path = "/var/log/cf_logs/"

    #check whether Zone ID is given by the user via the parameter. If not, check the environment variable.
    #if not in environment variable, then check the config file.
    #priority of reading Zone ID: arguments - environment variable - config file.
    #if no Zone ID is given, an error message will be given to the user and the program will exit
    if args.zone:
        zone_id = args.zone
    elif os.getenv("CF_ZONE_ID"):
        zone_id = os.getenv("CF_ZONE_ID")
    elif parsed_config.get("cf_zone_id"):
        zone_id = parsed_config.get("cf_zone_id")
    else:
        logger.critical(str(datetime.now()) + " --- Please specify your Cloudflare Zone ID.")
        sys.exit(2)
        
    #check whether Cloudflare Access Token is given by the user via the parameter. If not, check the environment variable.
    #if not in environment variable, then check the config file.
    #priority of reading Cloudflare Access Token: arguments - environment variable - config file.
    #if no Cloudflare Access Token is given, an error message will be given to the user and the program will exit
    if args.token:
        access_token = args.token
    elif os.getenv("CF_TOKEN"):
        access_token = os.getenv("CF_TOKEN")
    elif parsed_config.get("cf_token"):
        access_token = parsed_config.get("cf_token")
    else:
        logger.critical(str(datetime.now()) + " --- Please specify your Cloudflare Access Token.")
        sys.exit(2)
    
    #check if user provides the sample rate value in command line as argument, if not, check the config file.
    #if not exist in config file, use the default value.
    #priority of reading : arguments - config file - default value (1).
    if args.rate:
        sample_rate = args.rate
    elif parsed_config.get("rate"):
        sample_rate = parsed_config.get("rate")
    else:
        sample_rate = 1
    #check whether the sample rate is valid, if not return an error message and exit
    try:
        #the value should not more than two decimal places
        if len(str(sample_rate).split(".", 1)[1]) > 2:
            logger.critical(str(datetime.now()) + " --- Invalid sample rate specified. Please specify a value between 0.01 and 1, and only two decimal places allowed.")
            sys.exit(2)
    except IndexError:
        #sometimes the user may specify 1 as the value, so we need to handle the exception for value with no decimal places
        pass
    if sample_rate <= 1.0 and sample_rate >= 0.01:
        sample_rate = str(sample_rate)
    else:
        logger.critical(str(datetime.now()) + " --- Invalid sample rate specified. Please specify a value between 0.01 and 1, and only two decimal places allowed.")
        sys.exit(2)
    
    #if the user wants to do one-time operation, check the correctness of the start time and end time of the logs to pull.
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
                if diff_to_now.total_seconds() < 60:
                    logger.critical(str(datetime.now()) + " --- Please specify an end time that is 60 seconds or more earlier than the current time.")
                    sys.exit(2)
            except ValueError:
                logger.critical(str(datetime.now()) + " --- Invalid date format specified. Make sure it is in RFC 3339 date format, in UTC timezone. Please refer to the example: 2020-12-31T12:34:56Z")
                sys.exit(2)
        else:
            logger.critical(str(datetime.now()) + " --- No start time or end time specified for one-time operation. ")
            sys.exit(2)
    
    #check if user specifies interval in the command line as parameter. If not, check the config file. Else, use the default value.
    #priority of reading interval value: arguments - config file - default value (60).
    if args.interval:
        interval = args.interval
    elif parsed_config.get("interval"):
        interval = parsed_config.get("interval")
    else:
        interval = 60

    #check if user specifies prefix in the command line as parameter. If not, check the config file. Else, use the default value.
    #priority of reading prefix value: arguments - config file - default value (60).
    if args.prefix:
        logfile_name_prefix = args.prefix
    elif parsed_config.get("prefix"):
        logfile_name_prefix = parsed_config.get("prefix")
    else:
        logfile_name_prefix = "cf_logs"

    #if the user specifies True either as command line arguments or inside config file, then we assume the user wants to turn on the option.
    no_organize = True if args.no_organize is True or parsed_config.get("no_organize") is True else False
    no_gzip = True if args.no_gzip is True or parsed_config.get("no_gzip") is True else False

    #by default, we don't include Bot Management related fields for logpull - not all customers have Bot Management enabled in their zone.
    #if the user has Bot Management enabled and would like to enable Bot Management related fields, they can specify --bot-management as parameter.
    bot_management = True if args.bot_management is True or parsed_config.get("bot_management") is True else False
    general_fields += (bot_fields if bot_management is True else [])
    general_fields.sort()
    final_fields = ','.join(field for field in general_fields)


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
def prepare_path(log_start_time_rfc3339, log_end_time_rfc3339, data_folder, no_gzip):
    logfile_name = logfile_name_prefix + "_" + log_start_time_rfc3339 + "~" + log_end_time_rfc3339 + (".json" if no_gzip is True else ".json.gz")
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
This method is responsible to write logs to local storage after the logs have been pulled from Cloudflare API.
After successfully save the logs, it will also trigger compress_logs() method to compress the newly written logs.
'''
def write_logs(log_start_time_rfc3339,  log_end_time_rfc3339, logfile_path, data, no_gzip):
    try:
        if no_gzip is True:
            #open the file as write mode if user specifies not to compress the logs. Save the logs from decoded text response.
            logfile = open(logfile_path, mode="w", encoding="utf-8")
            logfile.write(data.text)
        else:
            #open the file as write binary mode to save the logs from raw gzipped response.
            logfile = open(logfile_path, mode="wb")
            logfile.write(data.raw.read())
        logfile.close()
    except:
        return False
    
    return True

          
'''
This method will handle the overall log processing tasks and it will run as a separate thread.
Based on the interval setting configured by the user, this method will only handle logs for a specific time slot.
'''
def logs(current_time, log_start_time_utc, log_end_time_utc):
    
    global path, num_of_running_thread, logger, retry_attempt, no_organize, no_gzip, final_fields, bot_management
    
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
    logfile_path = prepare_path(log_start_time_rfc3339, log_end_time_rfc3339, data_folder, no_gzip)

    #check the returned value from prepare_path() method. if False, means logfile already exists and no further action required
    if logfile_path is False:

        logger.warning(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Logfile already exists! Skipping.")

        return check_if_exited()
    
    #specify the URL for the Cloudflare API endpoint, with parameters such as Zone ID, the start time and end time of the logs to pull, timestamp format, sample rate and the fields to be included in the logs
    url = "https://api.cloudflare.com/client/v4/zones/" + zone_id + "/logs/received?start=" + log_start_time_rfc3339 + "&end=" + log_end_time_rfc3339 + "&timestamps="+ timestamp_format +"&sample=" + sample_rate + "&fields=" + final_fields

    #specify headers for the content type and access token 
    if no_gzip is True:
        headers = {"Authorization": "Bearer " + access_token, "Content-Type": "application/json", 'User-Agent': 'cf-logs-downloader (https://github.com/erictung1999/cf-logs-downloader)'}
    else:
        headers = {"Authorization": "Bearer " + access_token, "Content-Type": "application/json", "Accept-Encoding": "gzip", 'User-Agent': 'cf-logs-downloader (https://github.com/erictung1999/cf-logs-downloader)'}
    

    logger.info(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Requesting logs from Cloudflare...")
    
    #5 retries will be given for the logpull process, in case something happens
    for i in range(retry_attempt+1):
        #make a GET request to the Cloudflare API
        r = requests.get(url, headers=headers, stream=True)
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
                logger.error(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Unknown error occured while pulling logs with error code " + str(r.status_code) + ". Error dump: " + r.text + ". " + (("Retrying " + str(i+1) + " of " + str(retry_attempt) + "...") if i < (retry_attempt) else ""))
                time.sleep(3)
                continue

            #to check whether "success" key exists in JSON object, if yes, check whether the value is False, and print out the error message
            if "success" in response:
                if response["success"] is False:
                    logger.error(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Failed to request logs from Cloudflare with error code " + str(response["errors"][0]["code"]) + ": " + response["errors"][0]["message"] + ". " + ("Do you have Bot Management enabled in your zone?" if response["errors"][0]["code"] == 1010 and bot_management is True else ("Retrying " + str(i+1) + " of " + str(retry_attempt) + "...") if i < (retry_attempt) else ""))
                    if bot_management is True:
                        break
                    time.sleep(3)
                    continue
                else:
                    #something weird happened if it is not False. If the request has been successfully done, it should not return this kind of error, instead the raw logs should be returned with HTTP response code 200.
                    logger.error(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Unknown error occured while pulling logs with error code " + str(r.status_code) + ". Error dump: " + r.text + ". " + (("Retrying " + str(i+1) + " of " + str(retry_attempt) + "...") if i < (retry_attempt) else ""))
                    time.sleep(3)
                    continue
            else:
                #other type of error may occur, which may not return a JSON object.
                logger.error(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Unknown error occured while pulling logs with error code " + str(r.status_code) + ". Error dump: " + r.text + ". " + (("Retrying " + str(i+1) + " of " + str(retry_attempt) + "...") if i < (retry_attempt) else ""))
                time.sleep(3)
                continue
            
    #check whether the logpull process from Cloudflare API has been successfully completed, if yes then proceed with next steps
    if request_success is False:
        fail_logger.error("Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + " (Logpull error)")
        return check_if_exited()

    #Proceed to save the logs
    logger.info(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Logs requested. Saving logs...")
    if write_logs(log_start_time_rfc3339,  log_end_time_rfc3339, logfile_path, r, no_gzip):
        #successful of write logs
        logger.info(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Logs " + ("without gzip compression" if no_gzip is True else "compressed with gzip") + " saved as " + str(logfile_path) + ". ")
    else:
        #unsuccessful of write logs
        logger.error(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Failed to save logs to local storage.")
        fail_logger.error("Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + " (Write log error)")
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
        
