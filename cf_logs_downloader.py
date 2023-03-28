#!/usr/bin/env python3

#import libraries needed in this program
#'requests' library needs to be installed first
import requests, time, threading, os, json, logging, sys, argparse, logging.handlers, yaml, yschema, tempfile, signal, persistqueue
from datetime import datetime, timedelta
from pathlib import Path
from shutil import copy2
from gzip import decompress, compress

#specify version number of the program
ver_num = "2.8.2"

#a flag to determine whether the user wants to exit the program, so can handle the program exit gracefully
is_exit = False

#determine how many logpull process are running
num_of_running_thread = 0

#define the timestamp format that we supply to Cloudflare API
timestamp_format = "rfc3339"

#the defaut sampling rate for the logs
sample_rate = 1

#initialize the variables to empty string, so the other parts of the program can access it
log_type = zone_id = account_id = api_token = start_time = end_time = fields = final_fields = log_dest = ""

#the default value for the interval between each logpull process
interval = 60

#set the below settings to default: False
one_time = hide_user_logs = False

#specify the path to install the systemd service
service_path = '/etc/systemd/system/cf-logs-downloader.service'

'''
Specify the fields for the logs

The following fields are available for HTTP: BotScore,BotScoreSrc,CacheCacheStatus,CacheResponseBytes,CacheResponseStatus,CacheTieredFill,ClientASN,ClientCountry,ClientDeviceType,ClientIP,ClientIPClass,ClientRequestBytes,ClientRequestHost,ClientRequestMethod,ClientRequestPath,ClientRequestProtocol,ClientRequestReferer,ClientRequestURI,ClientRequestUserAgent,ClientSSLCipher,ClientSSLProtocol,ClientSrcPort,ClientXRequestedWith,EdgeColoCode,EdgeColoID,EdgeEndTimestamp,EdgePathingOp,EdgePathingSrc,EdgePathingStatus,EdgeRateLimitAction,EdgeRateLimitID,EdgeRequestHost,EdgeResponseBytes,EdgeResponseCompressionRatio,EdgeResponseContentType,EdgeResponseStatus,EdgeServerIP,EdgeStartTimestamp,FirewallMatchesActions,FirewallMatchesRuleIDs,FirewallMatchesSources,OriginIP,OriginResponseHTTPExpires,OriginResponseHTTPLastModified,OriginResponseStatus,OriginResponseTime,OriginSSLProtocol,ParentRayID,RayID,RequestHeaders,SecurityLevel,WAFAction,WAFProfile,WAFRuleID,WAFRuleMessage,WorkerCPUTime,WorkerStatus,WorkerSubrequest,WorkerSubrequestCount,ZoneID
The following fields are available for Cloudflare Access: action,allowed,app_domain,app_name,app_type,app_uid,connection,country,created_at,ip_address,purpose_justification_prompt,purpose_justification_response,ray_id,temporary_access_approvers,temporary_access_duration,user_email,user_id

Deprecated log fields: OriginResponseBytes,WAFFlags,WAFMatchedVar
'''
http_fields = ["BotScore","BotScoreSrc","CacheCacheStatus","CacheResponseBytes","CacheResponseStatus","CacheTieredFill","ClientASN","ClientCountry","ClientDeviceType","ClientIP","ClientIPClass","ClientRequestBytes","ClientRequestHost","ClientRequestMethod","ClientRequestPath","ClientRequestProtocol","ClientRequestReferer","ClientRequestURI","ClientRequestUserAgent","ClientSSLCipher","ClientSSLProtocol","ClientSrcPort","ClientXRequestedWith","EdgeColoCode","EdgeColoID","EdgeEndTimestamp","EdgePathingOp","EdgePathingSrc","EdgePathingStatus","EdgeRateLimitAction","EdgeRateLimitID","EdgeRequestHost","EdgeResponseBytes","EdgeResponseCompressionRatio","EdgeResponseContentType","EdgeResponseStatus","EdgeServerIP","EdgeStartTimestamp","FirewallMatchesActions","FirewallMatchesRuleIDs","FirewallMatchesSources","OriginIP","OriginResponseHTTPExpires","OriginResponseHTTPLastModified","OriginResponseStatus","OriginResponseTime","OriginSSLProtocol","ParentRayID","RayID","RequestHeaders","SecurityLevel","WAFAction","WAFProfile","WAFRuleID","WAFRuleMessage","WorkerCPUTime","WorkerStatus","WorkerSubrequest","WorkerSubrequestCount","ZoneID"]
access_fields = ["action","allowed","app_domain","app_name","app_type","app_uid","connection","country","created_at","ip_address","purpose_justification_prompt","purpose_justification_response","ray_id","temporary_access_approvers","temporary_access_duration","user_email","user_id"]

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
logger.addHandler(handler_console)
succ_logger.addHandler(succ_handler_file)
fail_logger.addHandler(fail_handler_file)

#create a SQLite queue system to handle failed tasks
queue = persistqueue.SQLiteQueue('/var/log/cf_logs_downloader/queue/', auto_commit=True, multithreading=True)

#create a threading event for wait() function
event = threading.Event()

'''
This is the starting point of the program. It will initialize the parameters supplied by the user and save it in a variable.
Help(welcome) message will be displayed if the user specifies -h or --help as the parameter.
If required parameters are not given by the user, an error message will be displayed to the user and the program will exit.
'''
def initialize_arg():
    
    global log_type, zone_id, account_id, api_token, sample_rate, interval, logger, start_time_static, end_time_static, one_time, fields, final_fields, yaml_schema, log_dest, hide_user_logs
    
    welcome_msg = "A little tool to pull/download HTTP, Cloudflare Access and Audit logs from Cloudflare and save it on local storage."

    parsed_config = {}

    #create an argparse object with the welcome message as the description
    parser = argparse.ArgumentParser(description=welcome_msg)
    
    #specify which arguments are available to use in this program. The usage of the arguments will be printed when the user tells the program to display help message.
    parser.add_argument("-c", "--config", metavar="config.yml", help="Specify the path to the YAML configuration file.")
    parser.add_argument("-a", "--account", metavar="ACCOUNT_ID", help="Specify the Cloudflare Account ID, if CF_ACCOUNT_ID environment variable not set. This will override CF_ACCOUNT_ID variable. Use only with 'access' or 'audit' log type.")
    parser.add_argument("-z", "--zone", metavar="ZONE_ID", help="Specify the Cloudflare Zone ID, if CF_ZONE_ID environment variable not set. This will override CF_ZONE_ID variable. Use only with 'http' log type.")
    parser.add_argument("-t", "--token", help="Specify your Cloudflare API Token, if CF_TOKEN environment variable not set. This will override CF_TOKEN variable.")
    parser.add_argument("-r", "--rate", help="Specify the log sampling rate from 0.01 to 1. Default is 1. Only applicable for 'http' log type.", type=float)
    parser.add_argument("-i", "--interval", help="Specify the interval between each logpull in seconds. Default is 60 seconds.", type=int)
    parser.add_argument("-n", "--nice", help="Specify the niceness of the logpull process from -20 (highest priority) to 19 (lowest priority). Default is -10.", type=int)
    parser.add_argument("--type", help="Specify the type of logs that you would like to pull. Possible values: http (for HTTP logs), access (for Cloudflare Access logs), audit (for Cloudflare Audit logs)")
    parser.add_argument("--path", metavar="/log/path/", help="Specify the path to store logs. By default, it will save to /var/log/cf_logs/.")
    parser.add_argument("--prefix", help="Specify the prefix name of the logfile being stored on local storage. By default, the file name will begins with cf_logs.")
    parser.add_argument("--no-organize", help="Instruct the program to store raw logs as is, without organizing them into date and time folder.", action="store_true")
    parser.add_argument("--no-gzip", help="Do not compress the raw logs.", action="store_true")
    parser.add_argument("--hide-user-logs", help="Enable this option if you prefer not to include user activity logs inside the audit logs, which includes user login histories, API-related events, etc. Only use this parameter with '--type audit'.", action="store_true")
    parser.add_argument("--one-time", help="Only pull logs from Cloudflare for one time, without scheduling capability. You must specify the start time and end time of the logs to be pulled from Cloudflare.", action="store_true")
    parser.add_argument("--start-time", help="Specify the start time of the logs to be pulled from Cloudflare. The start time is inclusive. You must follow the ISO 8601 (RFC 3339) date format, in UTC timezone. Example: 2020-12-31T12:34:56Z")
    parser.add_argument("--end-time", help="Specify the end time of the logs to be pulled from Cloudflare. The end time is exclusive. You must follow the ISO 8601 (RFC 3339) date format, in UTC timezone. Example: 2020-12-31T12:35:00Z")
    parser.add_argument("--exclude", metavar="field1,field2", help="Specify the list of log fields to be excluded from Logpull. Separate each field by comma without spaces. Only applicable for 'http' log type.")
    parser.add_argument("--available-fields", metavar="TYPE", help="Specify the log type to display the list of available log fields used by the program. These fields are also included in the logpull by default (unless field exclusion is configured). Possible values: http | access.")
    parser.add_argument("--install-service", help="Install the program as a systemd service. The service will execute the program from the path where you install the service.", action="store_true")
    parser.add_argument("--uninstall-service", help="Uninstall the systemd service.", action="store_true")
    parser.add_argument("--list-queue", help="List all the pending tasks in the queue which has failed before, without beautifying the result (raw JSON).", action="store_true")
    parser.add_argument("--list-queue-beauty", help="List all the pending tasks in the queue which has failed before, with beautifying the result.", action="store_true")
    parser.add_argument("--queue-size", help="Display the number of pending tasks in the queue which has failed before.", action="store_true")
    parser.add_argument("--debug", help="Enable debugging functionality.", action="store_true")
    parser.add_argument("-v", "--version", help="Show program version.", action="version", version="Version " + ver_num)
    
    #parse the parameters supplied by the user, and check whether the parameters match the one specified above
    #if it does not match, an error message will be given to the user and the program will exit
    args = parser.parse_args()

    one_time = args.one_time

    #only allow writing activity logs to disk when the user does not use one time operation.
    if one_time is False:
        logger.addHandler(handler_file)

    #if user specifies this parameter, list the queue as it is without any beautification and sorting
    if args.list_queue:
        print(json.dumps(queue.queue(), default=str))
        sys.exit(0)

    #if user specifies this parameter, list the queue with beautification and sorting based on log_start_time_utc
    if args.list_queue_beauty:
        print(json.dumps(sorted(queue.queue(), key=sort_json_by_log_start_time_utc), default=str, indent=2))
        sys.exit(0)

    #if user specifies this parameter, display the current size of the queue (how many items in the queue)
    if args.queue_size:
        print(str(queue.size))
        sys.exit(0)

    #catch someone who tries to "install and uninstall" service, which is definitely not logic.
    if args.install_service and args.uninstall_service:
        logger.critical(str(datetime.now()) + " --- Hold on. Are you trying to install or uninstall service?")
        sys.exit(2)

    #attempt to install service as requested by the user
    if args.install_service:
        #the user can also specify the location of the existing config file so that the config file can be copied directly to /etc/cf-logs-downloader/.
        config_path = args.config if args.config else False
        install_service(config_path)

    #attempt to uninstall service as requested by the user
    if args.uninstall_service:
        uninstall_service()

    #return the list of available fields by joining each field together as a string with ',' as delimiter
    if args.available_fields:
        if args.available_fields == 'http':
            print(','.join(field for field in http_fields))
            sys.exit(0)
        elif args.available_fields == 'access':
            print(','.join(field for field in access_fields))
            sys.exit(0)
        else:
            logger.critical(str(datetime.now()) + " --- No log fields for log type '" + log_type + "'. Valid values: http | access")
            sys.exit(2)
        
    #check if user specifies the path to configuration file, if yes, attempt read settings from the configuration file
    if args.config:
        #check if configuration file exists. if not, display an error and exit.
        try:
            config_file = open(args.config, mode="r", encoding="utf-8")
        except Exception as e:
            logger.critical(str(datetime.now()) + " --- Error while opening " + args.config + ": " + str(e) + ".")
            sys.exit(2)

        #if able to open the configuration file, load and parse the YAML data into Python dictionary.
        #if unable to parse the YAML data, display an error and exit.
        try:
            parsed_config = yaml.safe_load(config_file)
        except Exception as e:
            logger.critical(str(datetime.now()) + " --- Error parsing configuration file: " + str(e))
            sys.exit(2)
        finally:
            config_file.close()

        #retrieve the YAML schema from the schema file
        yaml_schema = get_yaml_schema()

        #check if the configuration follows the schema. If not, display an error and exit.
        try:
            yschema.validate(parsed_config, yaml_schema)
        except yschema.exceptions.ValidationError as e:
            logger.critical(str(datetime.now()) + " --- Error in configuration file: " + str(e) + ". Please check whether the settings are correct.")
            sys.exit(2)

    #enable debugging if specified by the user
    if args.debug is True or parsed_config.get("debug") is True:
        logger.setLevel(logging.DEBUG)

    #check whether the log type is specified by the user via the parameter. If not, check the environment variable.
    #if not in environment variable, then check the config file.
    #priority of reading log type: arguments - environment variable - config file.
    #if no log type is specified, an error message will be given to the user and the program will exit
    if args.type:
        log_type = args.type
    elif os.getenv("CF_LOG_TYPE"):
        log_type = os.getenv("CF_LOG_TYPE")
    elif parsed_config.get("type"):
        log_type = parsed_config.get("type")
    else:
        logger.critical(str(datetime.now()) + " --- Please specify the type of logs you want to pull. Possible values: http | access | audit")
        sys.exit(2)

    #check either zone ID or account ID based on the log type the user specified. HTTP logs only require zone ID, while Cloudflare Access and Audit logs only require account ID.
    if log_type == "http":
        #immediately assign the http fields list to a new variable, future reference of log fields will be the new variable
        fields = http_fields
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
        
        #check if user provides the sample rate value in command line as argument, if not, check the config file.
        #if not exist in config file, use the default value.
        #priority of reading : arguments - config file - default value (1).
        if args.rate:
            sample_rate = args.rate
        elif parsed_config.get("rate"):
            sample_rate = parsed_config.get("rate")
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
        
        #display a warning to the user if the user specifies 'hide_user_logs' parameter while using Cloudflare HTTP log type.
        if args.hide_user_logs or parsed_config.get("hide_user_logs"):
            logger.warning(str(datetime.now()) + " --- 'hide_user_logs' parameter does not apply to Cloudflare Access logs. 'hide_user_logs' will be ignored.")

    elif log_type == "access":
        #immediately assign the Access fields list to a new variable, future reference of log fields will be the new variable
        fields = access_fields
        #check whether Account ID is given by the user via the parameter. If not, check the environment variable.
        #if not in environment variable, then check the config file.
        #priority of reading Account ID: arguments - environment variable - config file.
        #if no Account ID is given, an error message will be given to the user and the program will exit
        if args.account:
            account_id = args.account
        elif os.getenv("CF_ACCOUNT_ID"):
            account_id = os.getenv("CF_ACCOUNT_ID")
        elif parsed_config.get("cf_account_id"):
            account_id = parsed_config.get("cf_account_id")
        else:
            logger.critical(str(datetime.now()) + " --- Please specify your Cloudflare Account ID.")
            sys.exit(2)

        #display a warning to the user if the user specifies sample rate or 'hide_user_logs' parameter while using Cloudflare Access log type.
        if args.rate or parsed_config.get("rate"):
            logger.warning(str(datetime.now()) + " --- Cloudflare Access log does not support sampling. Sample rate will be ignored.")
        if args.hide_user_logs or parsed_config.get("hide_user_logs"):
            logger.warning(str(datetime.now()) + " --- 'hide_user_logs' parameter does not apply to Cloudflare Access logs. 'hide_user_logs' will be ignored.")

    elif log_type == "audit":
        #check whether Account ID is given by the user via the parameter. If not, check the environment variable.
        #if not in environment variable, then check the config file.
        #priority of reading Account ID: arguments - environment variable - config file.
        #if no Account ID is given, an error message will be given to the user and the program will exit
        if args.account:
            account_id = args.account
        elif os.getenv("CF_ACCOUNT_ID"):
            account_id = os.getenv("CF_ACCOUNT_ID")
        elif parsed_config.get("cf_account_id"):
            account_id = parsed_config.get("cf_account_id")
        else:
            logger.critical(str(datetime.now()) + " --- Please specify your Cloudflare Account ID.")
            sys.exit(2)

        #check if user specifies the 'hide_user_logs' parameter in the command line, if not, check the config file.
        #if not exist in config file, use the default value.
        #priority of reading : arguments - config file - default value (False).
        if args.hide_user_logs:
            hide_user_logs = args.hide_user_logs
        elif parsed_config.get("hide_user_logs"):
            hide_user_logs = parsed_config.get("hide_user_logs")

        #display a warning to the user if the user specifies sample rate while using Cloudflare Audit log type.
        if args.rate or parsed_config.get("rate"):
            logger.warning(str(datetime.now()) + " --- Cloudflare Audit log does not support sampling. Sample rate will be ignored.")
    else:
        logger.critical(str(datetime.now()) + " --- Invalid log type '" + log_type + "'. Valid values: http | access | audit")
        sys.exit(2)
        
    #check whether Cloudflare API Token is given by the user via the parameter. If not, check the environment variable.
    #if not in environment variable, then check the config file.
    #priority of reading Cloudflare API Token: arguments - environment variable - config file.
    #if no Cloudflare API Token is given, an error message will be given to the user and the program will exit
    if args.token:
        api_token = args.token
    elif os.getenv("CF_TOKEN"):
        api_token = os.getenv("CF_TOKEN")
    elif parsed_config.get("cf_token"):
        api_token = parsed_config.get("cf_token")
    else:
        logger.critical(str(datetime.now()) + " --- Please specify your Cloudflare API Token.")
        sys.exit(2)
    
    #if the user wants to do one-time operation, check the correctness of the start time and end time of the logs to pull.
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
                if log_type == "http":
                    if diff_to_now.total_seconds() < 60:
                        logger.critical(str(datetime.now()) + " --- Please specify an end time that is 60 seconds or more earlier than the current time.")
                        sys.exit(2)
                elif log_type == "access" or log_type == "audit" :
                    if diff_to_now.total_seconds() < 1:
                        logger.critical(str(datetime.now()) + " --- Please specify an end time that is 1 second or more earlier than the current time.")
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

    #check if user specifies niceness in the command line as parameter. If not, check the config file. Else, use the default value.
    #priority of reading interval value: arguments - config file - default value (-10).
    #niceness value must be between -20 to 19.
    try:
        if args.nice:
            if args.nice < -20 :
                logger.warning(str(datetime.now()) + " --- The value of niceness is too small. Setting the value to -20.")
                os.nice(-20)
            elif args.nice > 19 :
                logger.warning(str(datetime.now()) + " --- The value of niceness is too large. Setting the value to 19.")
                os.nice(19)
            else:
                os.nice(args.nice)
        elif parsed_config.get("nice"):
            if parsed_config.get("nice") < -20 :
                logger.warning(str(datetime.now()) + " --- The value of niceness is too small. Setting the value to -20.")
                os.nice(-20)
            elif parsed_config.get("nice") > 19 :
                logger.warning(str(datetime.now()) + " --- The value of niceness is too large. Setting the value to 19.")
                os.nice(19)
            else:
                os.nice(parsed_config.get("nice"))
        else:
            os.nice(-10)
    except Exception as e:
        logger.warning(str(datetime.now()) + " --- Unable to set niceness value of the logpull process: " + str(e) + ".")

    #check if the user specifies log path and logfile prefix in command line arguments. If yes, override everything specified in the config file.
    if args.path or args.prefix:
        log_dest = [{'name': 'default', 'path': args.path if args.path else '/var/log/cf_logs/', 'prefix': args.prefix if args.prefix else 'cf_logs', 'no_organize': False, 'no_gzip': False}]
    #else if there's log destination configuration in config file, then get the value fron it
    elif parsed_config.get("log_dest"):
        log_dest = parsed_config.get("log_dest")
    #else, use the default value
    else:
        log_dest = [{'name': 'default', 'path': '/var/log/cf_logs/', 'prefix': 'cf_logs', 'no_organize': False, 'no_gzip': False}]

    #if the user specifies True either as command line arguments or inside config file, then we assume the user wants to turn on the option.
    for i in range(len(log_dest)):
        log_dest[i]['no_organize'] = True if args.no_organize is True else log_dest[i].get('no_organize')
        log_dest[i]['no_gzip'] = True if args.no_gzip is True else log_dest[i].get('no_gzip')
    
    #only perform field exclusion on HTTP log type
    if log_type == "http":
        #exclude certain fields in logpull
        if args.exclude:
            list_exclude_field = "".join(args.exclude.split()) #remove all whitespaces
            list_exclude_field = list_exclude_field.split(',') #each field will be separated by a comma, required by Cloudflare API logpull schema
            for exclude_field in list_exclude_field:
                fields.remove(exclude_field)
        elif parsed_config.get('fields.exclude'):
            for exclude_field in parsed_config.get('fields.exclude'):
                fields.remove(exclude_field)
        final_fields = ','.join(field for field in fields)
    elif log_type == "access":
        if args.exclude or parsed_config.get('fields.exclude'):
            logger.warning(str(datetime.now()) + " --- Cloudflare Access log does not support exclusion of log fields. All fields will be included in the log. Field exclusion will be ignored. Specify '--available-fields access' parameter to view the list of Cloudflare Access log fields.")
    elif log_type == "audit":
        if args.exclude or parsed_config.get('fields.exclude'):
            logger.warning(str(datetime.now()) + " --- Cloudflare Audit log does not support exclusion of log fields. All fields will be included in the log. Field exclusion will be ignored.")

'''
This method is to retrieve the YAML schema from the schema file (schema.yml), and return the value of the schema to the caller.
'''
def get_yaml_schema():
    try:
        yaml_schema_file = open('schema.yml', mode='r', encoding='utf-8')
        yaml_schema = yaml.safe_load(yaml_schema_file)
        yaml_schema_file.close()
        return yaml_schema
    except FileNotFoundError:
        logger.critical(str(datetime.now()) + " --- Unable to parse YAML schema: schema.yml file not found. Clone the repository from Github, or download the release file and try again.")
        sys.exit(2)
    except Exception as e:
        logger.critical(str(datetime.now()) + " --- Unable to parse YAML schema: " + str(e) + ". Clone the repository from Github, or download the release file and try again.")
        sys.exit(2)

'''
This method sorts the incoming JSON object (task queue) by log_start_time_utc.
'''
def sort_json_by_log_start_time_utc(value):
    return value["data"]["log_start_time_utc"]

'''
This method will install the tool as a systemd service.
'''
def install_service(config_path):
    service_desc = '''\
        [Unit]
        Description=A little tool to pull/download HTTP, Cloudflare Access and Audit logs from Cloudflare and save it on local storage.
        After=network.target
        StartLimitIntervalSec=0

        [Service]
        Type=simple
        Restart=always
        RestartSec=1
        User=root
        ExecStart={cwd}/cf_logs_downloader.py --config {config_file}

        [Install]
        WantedBy=multi-user.target\
    '''.format(cwd=os.getcwd(), config_file=config_path)

    try:
        #try write the service file
        service_file = open(service_path, mode='w', encoding="utf-8")
        service_file.write(service_desc)
        service_file.close()
        #reload the systemd after adding new service
        os.system("systemctl daemon-reload")
        #check if the user specifies the config file path. If yes, copy the config file and paste it into /etc/cf-logs-downloader/.
        if config_path:
            logger.info(str(datetime.now()) + " --- Successfully installed service as " + service_path + ".")
            try:
                copy2(config_path, '/etc/cf-logs-downloader/config.yml')
                logger.info(str(datetime.now()) + " --- Successfully copied the config file to /etc/cf-logs-downloader/config.yml.")
            except IOError as io_err:
                os.makedirs(os.path.dirname('/etc/cf-logs-downloader/'))
                copy2(config_path, '/etc/cf-logs-downloader/config.yml')
                logger.info(str(datetime.now()) + " --- Successfully copied the config file to /etc/cf-logs-downloader/config.yml.")
        else:
            logger.info(str(datetime.now()) + " --- Successfully installed service as " + service_path + ". Ensure that the config file is located in /etc/cf-logs-downloader/config.yml before you start the service.")
        logger.info(str(datetime.now()) + " --- Enable the service by using this command: systemctl enable cf-logs-downloader")
        logger.info(str(datetime.now()) + " --- Start the service by using this command: systemctl start cf-logs-downloader")
        sys.exit(0)
    except Exception as e:
        logger.critical(str(datetime.now()) + " --- Error while installing service as " + service_path + ":" + str(e) + ".")
        sys.exit(126)
        

'''
This method will uninstall the systemd service.
'''
def uninstall_service():
    if os.path.exists(service_path):
        try:
            #disable the service first before deleting the service.
            os.system("systemctl disable cf-logs-downloader")
            os.remove(service_path)
            #reload the systemd service after deleting the service.
            os.system("systemctl daemon-reload")
            logger.info(str(datetime.now()) + " --- Successfully uninstalled the service.")
            sys.exit(0)
        except Exception as e:
            logger.critical(str(datetime.now()) + " --- Error while uninstalling service:" + str(e) + ". You may remove the service manually by deleting " + service_path + ".")
            sys.exit(126)
    else:
        logger.critical(str(datetime.now()) + " --- The service was not installed previously. Abort.")
        sys.exit(126)


'''
This method will be invoked after initialize_arg().
This method is to verify whether the Cloudflare Zone ID/Account ID (depending on the log type) and Cloudflare API Token given by the user is valid.
If it is not valid, an error message will be given to the user and the program will exit
'''
def verify_credential():
    
    global logger

    if log_type == "http":
        #specify the Cloudflare API URL to check the Zone ID and API Token
        url = "https://api.cloudflare.com/client/v4/zones/" + zone_id + "/logs/received"
        headers = {"Authorization": "Bearer " + api_token, "Content-Type": "application/json"}

        #make a HTTP request to the Cloudflare API
        try:
            r = requests.get(url, headers=headers)
            r.encoding = "utf-8"
        except Exception as e:
            logger.critical(str(datetime.now()) + " --- Unable to perform API request to Cloudflare: " + str(e))
            sys.exit(2)
        
        #if there's an error, Cloudflare API will return a JSON object to indicate the error
        #and if it's not, a plain text will be returned instead
        #the try except block is to catch any errors raised by json.loads(), in case Cloudflare is not returning JSON object
        try:
            response = json.loads(r.text)
            if response["success"] is False:
                logger.critical(str(datetime.now()) + " --- Failed to authenticate with Cloudflare API. Please check your Zone ID and Cloudflare API Token.")
                sys.exit(2)
        except json.JSONDecodeError:
            #a non-JSON object returned by Cloudflare indicates that authentication successful
            pass
    elif log_type == 'access':
        #specify the Cloudflare API URL to check the Account ID and API Token
        url = "https://api.cloudflare.com/client/v4/accounts/" + account_id + "/access/logs/access_requests"
        headers = {"Authorization": "Bearer " + api_token, "Content-Type": "application/json"}
        
        #make a HTTP request to the Cloudflare API
        try:
            r = requests.get(url, headers=headers)
            r.encoding = "utf-8"
        except Exception as e:
            logger.critical(str(datetime.now()) + " --- Unable to perform API request to Cloudflare: " + str(e))
            sys.exit(2)
        
        #Cloudflare API should always return a JSON object to indicate whether the request is successful or not.
        #the try except block is to catch any errors raised by json.loads(), in case Cloudflare is not returning JSON object
        try:
            response = json.loads(r.text)
            if response["success"] is False:
                logger.critical(str(datetime.now()) + " --- Failed to authenticate with Cloudflare API. Please check your Account ID and Cloudflare API Token.")
                sys.exit(2)
            else:
                #no errors. Can proceed with logpull.
                pass
        except json.JSONDecodeError as e:
            logger.critical(str(datetime.now()) + " --- Unable to perform API request to Cloudflare: " + str(e))
    elif log_type == 'audit':
        #specify the Cloudflare API URL to check the Account ID and API Token
        url = "https://api.cloudflare.com/client/v4/accounts/" + account_id + "/audit_logs?per_page=1"
        headers = {"Authorization": "Bearer " + api_token, "Content-Type": "application/json"}
        
        #make a HTTP request to the Cloudflare API
        try:
            r = requests.get(url, headers=headers)
            r.encoding = "utf-8"
        except Exception as e:
            logger.critical(str(datetime.now()) + " --- Unable to perform API request to Cloudflare: " + str(e))
            sys.exit(2)
        
        #Cloudflare API should always return a JSON object to indicate whether the request is successful or not.
        #the try except block is to catch any errors raised by json.loads(), in case Cloudflare is not returning JSON object
        try:
            response = json.loads(r.text)
            if response["success"] is False:
                logger.critical(str(datetime.now()) + " --- Failed to authenticate with Cloudflare API. Please check your Account ID and Cloudflare API Token.")
                sys.exit(2)
            else:
                #no errors. Can proceed with logpull.
                pass
        except json.JSONDecodeError as e:
            logger.critical(str(datetime.now()) + " --- Unable to perform API request to Cloudflare: " + str(e))

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
def prepare_path(log_start_time_rfc3339, log_end_time_rfc3339, data_folder, logfile_name_prefix, no_gzip):
    logfile_name = logfile_name_prefix + "_" + log_start_time_rfc3339 + "~" + log_end_time_rfc3339 + (".json" if no_gzip is True else ".json.gz")
    logfile_path = data_folder / logfile_name
    
    if os.path.exists(str(logfile_path)):
        return logfile_path, False
    else:
        return logfile_path, True
    
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
        sys.exit(0)
    
    return False

'''
This method will be called if the process receives SIGINT or SIGTERM signal from the system.
The purpose is to gracefully terminate the program. 
This method will check if the number of running threads is 0 (means no logpull subprocess running), then it will display an info message showing that program exited gracefully.
This method also sets the is_exit flag so that other logpull subprocess can check this flag before they exit.
'''
def graceful_terminate(signum, frame):
    global is_exit, num_of_running_thread, event

    is_exit = True

    #stop all the sleep timers in other methods, particularly queue_thread()
    event.set()
    print("")
    logger.info(str(datetime.now()) + " --- " + signal.Signals(signum).name + " detected. Initiating program exit. Finishing up log download tasks...")
    if num_of_running_thread <= 0:
        logger.info(str(datetime.now()) + " --- Program exited gracefully.")
        
    sys.exit(0)

'''
This method is responsible to write logs to local storage after the logs have been pulled from Cloudflare API.
Depending on the user preference, logs might need to save in compressed gzip format.
'''
def write_logs(logfile_path, data, no_gzip):
    dirname, basename = os.path.split(logfile_path)
    try:
        if no_gzip is True:
            #open the temporary file as write mode if user specifies not to compress the logs. Save the logs from decoded text response.
            logfile = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", prefix=basename, dir=dirname)
            if log_type == "http":
                #write the decompressed data
                logfile.write(str(decompress(data).decode(encoding='utf-8')))
            elif log_type == 'access' or log_type == 'audit':
                #Cloudflare Access and Audit log does not compress by default. Can write to file directly.
                logfile.write(data)
            #after writing logs to temporary file, create a hard link from actual file to the temporary file
            os.link(logfile.name, logfile_path)
        else:
            #open the temporary file as write binary mode to save the logs from raw gzipped response.
            logfile = tempfile.NamedTemporaryFile(mode="wb", prefix=basename, dir=dirname)
            if log_type == "http":
                #write the compressed gzip data
                logfile.write(data)
            elif log_type == 'access' or log_type == 'audit':
                #Cloudflare Access and Audit log does not compress by default. Data compression needs to be applied first.
                logfile.write(compress(data.encode()))
            #after writing logs to temporary file, create a hard link from actual file to the temporary file
            os.link(logfile.name, logfile_path)
        #close the temporary file and it will automatically deleted
        logfile.close()
    except Exception as e:
        return False, e
    
    return True, True

'''
This method will be run as a separate thread
Its main responsibility is to pick up new tasks from the queue and perform the logpull tasks again.
'''
def queue_thread():
    global num_of_running_thread, queue, is_exit, event

    #ensure that this process is also counted as one running thread, useful to perform task cleanup while stopping the process
    num_of_running_thread += 1

    #failed count to check how many failed tasks
    failed_count = 0

    #wait for 5 seconds before starting the process below, doesn't make sense to check the queue immediately after running the tool
    event.wait(5)

    #keep below process in a loop until it's terminated by the user
    while True:
        try:
            #check whether the queue has any content (size larger than 0)
            if queue.size > 0:
                #get the item from the queue based on FIFO
                item = queue.get()

                #then run the logpull task again
                logger.info(str(datetime.now()) + " --- Retrying log range " + item.get('log_start_time_utc').isoformat() + "Z to " + item.get('log_end_time_utc').isoformat() + "Z from queue due to " + item.get('reason') + "... (currently " + str(queue.size) + " item(s) left in the queue)")
                null, status = logs_thread(item.get('folder_time'), item.get('log_start_time_utc'), item.get('log_end_time_utc'))

                #check the status returned from the logpull process, if True means the last logpull task has been successful
                if status is True:
                    failed_count = 0
                    event.wait(3)
                else:
                    #if not, increment the failed count counter, also check if the failed tasks more than or equal to 3
                    failed_count += 1
                    if failed_count >= 3:
                        #too many failed tasks, wait for 60 seconds and try again
                        event.wait(60)
                    else:
                        #else, just wait for 3 seconds
                        event.wait(3)
            else:
                #if no item in the queue, wait for 5 seconds and try again
                event.wait(5)

            #check if the user wants to stop the logpull process, if no then just continue the looping
            if is_exit is True:
                time.sleep(1)
                return check_if_exited()
            else:
                pass
        except Exception as e:
            logger.critical(str(datetime.now()) + " --- Queue thread failed unexpectedly. Exception message: " + str(e))
            continue

          
'''
This method will handle the overall log processing tasks and it will run as a separate thread.
Based on the interval setting configured by the user, this method will only handle logs for a specific time slot.
'''
def logs_thread(current_time, log_start_time_utc, log_end_time_utc):
    
    global num_of_running_thread, logger, retry_attempt, final_fields, log_dest, queue, one_time, hide_user_logs

    #a list to store list of objects - log destination configuration
    log_dest_per_thread = []
    log_dest_per_thread_final = []
    
    #add one to the variable to indicate number of running threads. useful to determine whether to exit the program gracefully
    num_of_running_thread += 1

    #specify the number of attempts to retry in the event of error
    #Note! Setting 0 prevents retrying logpull tasks as defined in below code. The process will be replaced by queue_thread() instead.
    retry_attempt = 0
    
    #a variable to check whether the request to Cloudflare API is successful.
    request_success = False

    #a variable to check whether we should skip adding failed items to the queue (based on situation)
    skip_add_queue = False

    status_code = 0
    cf_status_code = 0
    cf_err_msg = ""
    
    #if the user instructs the program to do logpull for only one time, the logs will not be stored in folder that follows the naming convention: date and time
    if one_time is True or (all(d.get('no_organize') is True for d in log_dest)):
        pass
    else:
        #get the current date and hour, these will be used to initialize the folder to store the logs
        today_date = str(current_time.date())
        current_hour = str(current_time.hour) + "00"
    
    #get the log start time and log end time in RFC3339 format, so Cloudflare API will understand it and pull the appropriate logs for us
    log_start_time_rfc3339 = log_start_time_utc.isoformat() + 'Z'
    log_end_time_rfc3339 = log_end_time_utc.isoformat() + 'Z'

    #iterate through the list of objects - log destination configuration
    for d in log_dest:
        #check if the user wants to do one-time operation, or instructs not to organize logs into date and time folder
        #if yes, leave the path value as it is
        if d.get('no_organize') is True or one_time is True:
            log_dest_per_thread.append({'name': d.get('name'), 'path': d.get('path'), 'prefix': d.get('prefix'), 'no_gzip': d.get('no_gzip')})
        #if not, modify the path to include date and time folder
        else:
            log_dest_per_thread.append({'name': d.get('name'), 'path': d.get('path') + "/" + today_date + "/" + current_hour, 'prefix': d.get('prefix'), 'no_gzip': d.get('no_gzip')})

    #iterate through the list of objects - log destination configuration
    for p in log_dest_per_thread:
        #create folder
        data_folder = initialize_folder(p.get('path'))

        #prepare the full path (incl. file name) to store the logs
        logfile_path, prepare_status = prepare_path(log_start_time_rfc3339, log_end_time_rfc3339, data_folder, p.get('prefix'), p.get('no_gzip'))

        #check the returned value from prepare_path() method. if False, means logfile already exists and no further action required
        if prepare_status is False:
            logger.warning(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Logfile " + str(logfile_path) + " already exists! Skipping.")
        else:
            log_dest_per_thread_final.append({'name': p.get('name'), 'path': logfile_path, 'no_gzip': p.get('no_gzip')})

    #check if the python list is empty. Empty list means the particular logpull operation can be skipped because the log file already exists in all destinations.
    if not log_dest_per_thread_final:
        logger.warning(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Logfile exists in all paths. Skipping.")
        return check_if_exited(), True

    if log_type == "http":
        #specify the URL for the Cloudflare API endpoint, with parameters such as Zone ID, the start time and end time of the logs to pull, timestamp format, sample rate and the fields to be included in the logs
        url = "https://api.cloudflare.com/client/v4/zones/" + zone_id + "/logs/received?start=" + log_start_time_rfc3339 + "&end=" + log_end_time_rfc3339 + "&timestamps="+ timestamp_format +"&sample=" + sample_rate + "&fields=" + final_fields

        #specify headers for the content type and API token. Only accept gzip as response.
        headers = {"Authorization": "Bearer " + api_token, "Content-Type": "application/json", "Accept-Encoding": "gzip", 'User-Agent': 'cf-logs-downloader (https://github.com/erictung1999/cf-logs-downloader)'}
        
        logger.info(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Requesting HTTP logs from Cloudflare...")
    elif log_type == 'access':
        #specify the URL for the Cloudflare API endpoint, with parameters such as Account ID and the start time and end time of the logs to pull
        url = "https://api.cloudflare.com/client/v4/accounts/" + account_id + "/access/logs/access_requests?since=" + log_start_time_rfc3339 + "&until=" + log_end_time_rfc3339 + "&limit=1000"

        #specify headers for the content type and API token. 
        headers = {"Authorization": "Bearer " + api_token, "Content-Type": "application/json", 'User-Agent': 'cf-logs-downloader (https://github.com/erictung1999/cf-logs-downloader)'}
        
        logger.info(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Requesting Cloudflare Access logs from Cloudflare...")
    elif log_type == 'audit':
        #specify the URL for the Cloudflare API endpoint, with parameters such as Account ID and the start time and end time of the logs to pull
        if hide_user_logs is True:
            url = "https://api.cloudflare.com/client/v4/accounts/" + account_id + "/audit_logs?since=" + log_start_time_rfc3339 + "&before=" + log_end_time_rfc3339 + "&limit=1000&hide_user_logs=true"
        else:
            url = "https://api.cloudflare.com/client/v4/accounts/" + account_id + "/audit_logs?since=" + log_start_time_rfc3339 + "&before=" + log_end_time_rfc3339 + "&limit=1000&hide_user_logs=false"
        
        #specify headers for the content type and API token. 
        headers = {"Authorization": "Bearer " + api_token, "Content-Type": "application/json", 'User-Agent': 'cf-logs-downloader (https://github.com/erictung1999/cf-logs-downloader)'}
        
        logger.info(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Requesting Cloudflare Audit logs from Cloudflare...")

    for i in range(retry_attempt+1):
        #make a GET request to the Cloudflare API
        try:
            r = requests.get(url, headers=headers, stream=True if log_type == "http" else False)
            r.encoding = 'utf-8'
        except Exception as e:
            logger.critical(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Unable to perform API request to Cloudflare: " + str(e) + ". " + (("Retrying " + str(i+1) + " of " + str(retry_attempt) + "...") if i < (retry_attempt) else ""))
            time.sleep(3)
            continue
        
        #check whether the HTTP response code is 200, if yes then logpull success and exit the loop
        status_code = r.status_code
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
                    logger.error(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Failed to request logs from Cloudflare with error code " + str(response["errors"][0]["code"]) + ": " + response["errors"][0]["message"] + ". " + ("Consider removing BotScore and BotScoreSrc fields if your zone does not have Bot Management enabled." if response["errors"][0]["code"] == 1010 and ('BotScore' in fields or 'BotScoreSrc' in fields) else ("Retrying " + str(i+1) + " of " + str(retry_attempt) + "...") if i < (retry_attempt) else ""))
                    cf_status_code = response["errors"][0]["code"]
                    cf_err_msg = response["errors"][0]["message"]
                    if response["errors"][0]["code"] == 1010 and ('BotScore' in fields or 'BotScoreSrc' in fields):
                        skip_add_queue = True
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
    if request_success is False and one_time is False:
        #check if there's a need to add failed tasks to queue, if no, just add it to the log
        if skip_add_queue is True:
            fail_logger.error("Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + " [" + log_type + "] (Logpull error - HTTP " + str(status_code) + (", Cloudflare " + str(cf_status_code) + " - " + cf_err_msg if cf_status_code != 0 else "") + ")")
        else:
            queue.put({'folder_time': current_time, 'log_start_time_utc': log_start_time_utc, 'log_end_time_utc': log_end_time_utc, 'log_type': log_type, 'reason': 'Logpull error (HTTP ' + str(status_code) + (", Cloudflare " + str(cf_status_code) + " - " + cf_err_msg if cf_status_code != 0 else "") + ')'})
        return check_if_exited(), False

    i = 0

    if log_type == "http":
        #get the raw response (gzipped content) and save it into a variable.
        gzip_resp = r.raw.read()
    elif log_type == 'access':
        json_resp = r.json()
        if (len(json_resp["result"]) <= 0):
            logger.warning(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": No Access logs during this time range. Will not write file to local storage. Skipping...")
            succ_logger.info("Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + " [" + log_type + "] (No Access logs to write)")
            return check_if_exited(), True
        json_string_resp = [json.dumps(record) for record in json_resp["result"]]
    elif log_type == 'audit':
        json_resp = r.json()
        if (len(json_resp["result"]) <= 0):
            logger.warning(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": No Audit logs during this time range. Will not write file to local storage. Skipping...")
            succ_logger.info("Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + " [" + log_type + "] (No Audit logs to write)")
            return check_if_exited(), True
        json_string_resp = [json.dumps(record) for record in json_resp["result"]]
    
    #Proceed to save the logs
    logger.info(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Logs requested. Saving logs...")

    #iterate through list of objects - log destination configuration
    for each_log_dest in log_dest_per_thread_final:
        i += 1
        logger.info(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Writing logs " + str(i) + " of " + str(len(log_dest_per_thread_final)) + " (" + each_log_dest.get('name') + ") to " + str(each_log_dest.get('path')) + " ...")

        #write logs to the destination as specified by the user, with the option for gzip
        if log_type == "http":
            result, e = write_logs(each_log_dest.get('path'), gzip_resp, each_log_dest.get('no_gzip'))
        elif log_type == "access" or log_type == "audit":
            result, e = write_logs(each_log_dest.get('path'), '\n'.join(json_string_resp) + '\n', each_log_dest.get('no_gzip'))
        if result is True:
            #successful of write logs
            logger.info(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Logs " + ("without gzip compression" if each_log_dest.get('no_gzip') is True else "compressed with gzip") + " (" + each_log_dest.get('name') + ") saved as " + str(each_log_dest.get('path')) + ". ")
        else:
            #unsuccessful of write logs
            logger.error(str(datetime.now()) + " --- Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + ": Failed to save logs to local storage (" + each_log_dest.get('name') + "): " + str(e))
            #add failed tasks to queue
            if one_time is False:
                queue.put({'folder_time': current_time, 'log_start_time_utc': log_start_time_utc, 'log_end_time_utc': log_end_time_utc, 'log_type': log_type, 'reason': 'Write log error (' + each_log_dest.get('name') + ')'})
            return check_if_exited(), False

    #only write success log if the operation is not one-time
    if one_time is False:
        succ_logger.info("Log range " + log_start_time_rfc3339 + " to " + log_end_time_rfc3339 + " [" + log_type + "] (" + each_log_dest.get('name') + ")")

    #invoke this method to check whether the user triggers program exit sequence
    return check_if_exited(), True

        
####################################################################################################       
        
#register signals with a method. the method will be triggered if the user sends a signal to the program (SIGINT and SIGTERM)        
signal.signal(signal.SIGINT, graceful_terminate)
signal.signal(signal.SIGTERM, graceful_terminate)

#This is where the real execution of the program begins. First it will initialize the parameters supplied by the user
initialize_arg()

#After the above execution, it will verify the Zone ID and API Token given by the user whether they are valid
verify_credential()

#if both Zone ID and API Token are valid, the logpull tasks will begin.
logger.info(str(datetime.now()) + " --- Cloudflare logs download tasks started. Log type: " + log_type)

#if the user instructs the program to do logpull for only one time, the program will not do the logpull jobs repeatedly
if one_time is True:
    threading.Thread(target=logs_thread, args=(None, start_time_static, end_time_static)).start()
else:
    #first get the current system time, both local and UTC time.
    #the purpose of getting UTC time is to facilitate the calculation of the start and end time to pull the logs from Cloudflare API
    #the purpose of getting local time is to generate a directory structure to store logs, separated by the date and time
    current_time_utc = datetime.utcnow()
    current_time = datetime.now()

    #calculate how many seconds to go back from current time to pull the logs. 
    if log_type == "http":
        #mininum 60 seconds difference to accommodate Cloudflare logs delay, and also add at least 60 seconds or more, based on interval
        logs_from = 60.0 + (((interval-1) // 60 * 60) + 60)
    elif log_type == "access" or log_type == "audit":
        #add at least 60 seconds or more, based on interval
        logs_from = 0.0 + (((interval-1) // 60 * 60) + 60)

    #calculate the start time to pull the logs from Cloudflare API
    log_start_time_utc = current_time_utc.replace(second=0, microsecond=0) - timedelta(seconds=logs_from)
    current_time = current_time.replace(second=0, microsecond=0) - timedelta(seconds=logs_from)

    #this is useful when we need to repeat the execution of a code block after a certain interval, in an accurate way
    #below code will explain the usage of this in detail
    initial_time = time.time()

    #create a new thread to handle failed tasks inside queue
    threading.Thread(target=queue_thread).start()

    #force the program to run indefinitely, unless the user stops it with Ctrl+C
    while True:        
        #calculate the end time to pull the logs from Cloudflare API, based on the interval value given by the user
        if log_type == "http":
            log_end_time_utc = log_start_time_utc + timedelta(seconds=interval)
        elif log_type == 'access' or log_type == "audit":
            #as Cloudflare Access & Audit log request API does not automatically exclude 1 second from end time like what Cloudflare Logpull API does,
            #we must manually subtract 1 second so that subsequent log requests will not overlap with the time
            log_end_time_utc = log_start_time_utc + timedelta(seconds=interval-1)

        #create a new thread to handle the logs processing. the target method is logs() and 3 parameters are supplied to this method
        threading.Thread(target=logs_thread, args=(current_time, log_start_time_utc, log_end_time_utc)).start()

        #assigning start and end time to the next iteration
        if log_type == "http":
            log_start_time_utc = log_end_time_utc
        elif log_type == 'access' or log_type == "audit":
            #adding 1 second back to the next iteration of start time, as previously 1 second deduction has been made
            log_start_time_utc = log_end_time_utc + timedelta(seconds=1)
        current_time = current_time + timedelta(seconds=interval)

        time.sleep(interval - ((time.time() - initial_time) % interval))
