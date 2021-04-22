# cf-logs-downloader
A little tool to pull/download HTTP Access logs from Cloudflare Enterprise Log Share (ELS) and save it on local storage.

## Prerequisites
- You must have an active Cloudflare Enterprise zone in order to use Cloudflare Enterprise Log Share (ELS).
- Make sure the zone that you want to pull logs from already has Log Retention enabled - refer to [this link](https://developers.cloudflare.com/logs/logpull-api/enabling-log-retention) on how to enable it.
- Make sure your Cloudflare user account has the permission to access Zone logs (particularly, Log Share Reader role). If you are unsure about that, contact your Administrator. If you are an Administrator already, no further action is required.
- You need to [create an API Token from the Cloudflare Dashboard](https://dash.cloudflare.com/profile/api-tokens) to allow access to logs.
- Requires root access to your local machine.
- Currently only supports Linux. Windows isn't supported yet.

## Getting started
1. Download the latest version of the binary file from the [GitHub Releases section](https://github.com/erictung1999/cf-logs-downloader/releases).
3. Unzip the file. You should see 3 files: `cf_logs_downloader`, `schema.yml` and `sampleconfig.yml`.
4. Make "cf_logs_downloader" executable - `chmod +x cf_logs_downloader`.
5. Verify the script is working by executing `./cf_logs_downloader -v`. You should see this:

	```
	Version 2.5.2
	```

## Create an API Token
Follow the instructions below to generate an API token:
1. Go to https://dash.cloudflare.com/profile/api-tokens (you need to login first, of course!)
2. Click Create Token.
3. Under Custom Token section, click Get Started.
4. Give your API Token a name.
5. Under Permissions, choose Zone - Logs - Read.
6. Under Zone Resources, choose the specific zone that you want to read the logs from. Allowing access to all zones is not recommended.
7. Under IP Address Filtering, enter the source IP address that will call the Cloudflare API (recommended).
8. Provide a TTL to define how long this token can stay active (recommended).
9. Click Continue to Summary, then click Create Token.
10. Keep the generated token in a safe location. 

## Using the tool
Here are the list of parameters that you can leverage within the tool:
```
  -h, --help            show this help message and exit
  -c config.yml, --config config.yml
                        Specify the path to the YAML configuration file.
  -z ZONE_ID, --zone ZONE_ID
                        Specify the Cloudflare Zone ID, if CF_ZONE_ID
                        environment variable not set. This will override
                        CF_ZONE_ID variable.
  -t TOKEN, --token TOKEN
                        Specify your Cloudflare Access Token, if CF_TOKEN
                        environment variable not set. This will override
                        CF_TOKEN variable.
  -r RATE, --rate RATE  Specify the log sampling rate from 0.01 to 1. Default
                        is 1.
  -i INTERVAL, --interval INTERVAL
                        Specify the interval between each logpull in seconds.
                        Default is 60 seconds.
  -n NICE, --nice NICE  Specify the niceness of the logpull process from -20
                        (highest priority) to 19 (lowest priority). Default is
                        -10.
  --path /log/path/     Specify the path to store logs. By default, it will
                        save to /var/log/cf_logs/.
  --prefix PREFIX       Specify the prefix name of the logfile being stored on
                        local storage. By default, the file name will begins
                        with cf_logs.
  --no-organize         Instruct the program to store raw logs as is, without
                        organizing them into date and time folder.
  --no-gzip             Do not compress the raw logs.
  --bot-management      Specify this parameter if your zone has Bot Management
                        enabled and you want to include Bot Management related
                        fields in your logs.
  --one-time            Only pull logs from Cloudflare for one time, without
                        scheduling capability. You must specify the start time
                        and end time of the logs to be pulled from Cloudflare.
  --start-time START_TIME
                        Specify the start time of the logs to be pulled from
                        Cloudflare. The start time is inclusive. You must
                        follow the ISO 8601 (RFC 3339) date format, in UTC
                        timezone. Example: 2020-12-31T12:34:56Z
  --end-time END_TIME   Specify the end time of the logs to be pulled from
                        Cloudflare. The end time is exclusive. You must follow
                        the ISO 8601 (RFC 3339) date format, in UTC timezone.
                        Example: 2020-12-31T12:35:00Z
  --install-service     Install the program as a systemd service. The service
                        will execute the program from the path where you
                        install the service.
  --uninstall-service   Uninstall the systemd service.
  --list-queue          List all the pending tasks in the queue which has
                        failed before, without beautifying the result (raw
                        JSON).
  --list-queue-beauty   List all the pending tasks in the queue which has
                        failed before, with beautifying the result.
  --queue-size          Display the number of pending tasks in the queue which
                        has failed before.
  --debug               Enable debugging functionality.
  -v, --version         Show program version.
```

## Configuration file format
This tool supports specifying the settings via YAML configuration file. Refer to the list below for the supported settings:
1. `cf_zone_id` (string, optional) - Specify the Cloudflare Zone ID. 
2. `cf_token` (string, optional) - Specify the Cloudflare Access Token.
3. `rate` (float, optional) - Specify log sampling rate from 0.01 to 1. Default is 1.
4. `interval` (int, optional) - Specify the interval between each logpull in seconds. Default is 60 seconds.
5. `bot_management` (boolean, optional) - Specify this parameter if your zone has Bot Management enabled and you want to include Bot Management related fields in your logs. Acceptable values: `true` or `false`.
6. `nice` (int, optional) - Specify the niceness of the logpull process from -20 (highest priority) to 19 (lowest priority). Default is -10.
7. `debug` (boolean, optional) -  Enable debugging functionality. Acceptable values: `true` or `false`.
8.  `log_dest` (list, optional) - Specify this to further configure the settings for the destination of the logs. This includes multiple options as shown below:
	*  `name` (string, required) - Give a unique name of the log destination configuration. Useful to identify in activity log.
	* `path` (string, required) - Specify the path to store logs. By default, it will save to /var/log/cf_logs/
	* `prefix` (string, required) - Specify the prefix name of the logfile being stored on local storage. By default, the file name will begins with cf_logs.
	* `no_organize` (boolean, required) - Instruct the program to store raw logs as is, without organizing them into date and time folder. Acceptable values: `true` or `false`.
	* `no_gzip` (boolean, required) - Do not compress the raw logs. Acceptable values: `true` or `false`.

You may refer to schema.yml for more information.

Here's the sample of the configuration settings:
```
cf_zone_id: your_zone_id_here
cf_token: your_token_here
rate: 0.5
interval: 30
bot_management: true
nice: -10
debug: true
log_dest:
  - name: first_dest
    path: /var/log/first_path
    prefix: number_one
    no_organize: false
    no_gzip: true
  - name: second_dest
    path: /var/log/second_path
    prefix: number_two
    no_organize: true
    no_gzip: false
```

## Environment variables
Here are some environment variables that you can create while using this tool:
1. `CF_ZONE_ID` - Specify the Cloudflare Zone ID. 
2. `CF_TOKEN` - Specify the Cloudflare Access Token. 

## Precedence of configuration options
Usually command line arguments will take the highest priority among the others. However, depends on the settings, some of them might have different order of precedence:
1. **For Cloudflare Zone ID and Cloudflare Access Token:** command line arguments - environment variable - configuration file
2. **For sample rate, logpull interval and niceness:** command line arguments - configuration file - default value
3. **For bot management and debug option**: the option will be turned on when the user specifies it either as command line arguments or inside the configuration file.
4. **For log path and log file name prefix**: specifying this option as command line arguments will override everything specified under `log_dest` inside the configuration file.
5.  **For no organize and no gzip**: specifying this option as command line arguments will override `no_gzip` and `no_organize` option in each item under `log_dest` inside the configuration file.


## Example usage
1. At a bare minimum, you must specify Cloudflare Zone ID and Access Token while using the tool. By doing so, this tool will use default values for the below configurations:

	* Log sampling rate: 100% (represented by 1 in Cloudflare API)
	* Logpull interval: 60 seconds
	* Logpull storage path: `/var/log/cf_logs/`
	* Log filename prefix: `cf_logs`
	* Enable folder organize by date and time
	* Enable Gzip compression
	* No Bot Management fields (BotScore & BotScoreSrc) included in logpull
	* Niceness: -10
	* No debugging

2. To use the default configurations, you can use this command for logpull: 

	```
	$ sudo ./cf_logs_downloader.py -z YOUR_ZONE_ID -t YOUR_API_TOKEN
	```
	
	Or, if you wish to use the environment variable to specify Cloudflare Zone ID and API Token, just export the environment variable with the value assigned to it:

	```
	$ sudo su
	# export CF_ZONE_ID=YOUR_ZONE_ID
	# export CF_TOKEN=YOUR_API_TOKEN
	# ./cf_logs_downloader.py
	```
	
	Expected outcome: your logs will be stored in `/var/log/cf_logs/1970-01-01/1800/cf_logs_1970-01-01T18:00:00Z~1970-01-01T18:01:00Z.json.gz` initially. Subsequent logs will be stored in their respective folder based on date and time.

3. To pull logs with 10% sampling rate and 10 seconds of interval:

	```
	$ sudo ./cf_logs_downloader.py -z YOUR_ZONE_ID -t YOUR_API_TOKEN -r 0.1 -i 10
	```
	
	Expected outcome: your logs will be stored in `/var/log/cf_logs/1970-01-01/1800/cf_logs_1970-01-01T18:00:00Z~1970-01-01T18:00:10Z.json.gz` initially. Subsequent logs will be stored in their respective folder based on date and time.

4. To instruct the tool to include Bot Management fields (BotScore and BotScoreSrc), not to save the logs in compressed (gzip) format, and store the logs in a different folder:

	```
	$ sudo ./cf_logs_downloader.py -z YOUR_ZONE_ID -t YOUR_API_TOKEN --bot-management --no-gzip --path /root/Downloads/my_cloudflare_log/
	```

	Expected outcome: your logs will be stored in `/root/Downloads/my_cloudflare_log/1970-01-01/1800/cf_logs_1970-01-01T18:00:00Z~1970-01-01T18:01:00Z.json` initially. Subsequent logs will be stored in their respective folder based on date and time.

5. To pull the logs at 2 minutes of interval, and instruct the tool not to organize the logs in date/time folder and use a different prefix for the log filename:

	```
	$ sudo ./cf_logs_downloader.py -z YOUR_ZONE_ID -t YOUR_API_TOKEN -i 120 --no-organize --prefix my_site_log
	```

	Expected outcome: your logs will be stored in `/var/log/cf_logs/my_site_log_1970-01-01T18:00:00Z~1970-01-01T18:02:00Z.json.gz` initially. Subsequent logs will be stored in their respective folder based on date and time.

6. To pull the logs for just one time (without scheduling) and without organizing the log file into date/time folder:

	```
	$ sudo ./cf_logs_downloader.py -z YOUR_ZONE_ID -t YOUR_API_TOKEN --one-time --no-organize --start-time 2021-02-02T18:00:00Z --end-time 2021-02-02T18:30:00Z
	```

	Expected outcome: your log will be stored in `/var/log/cf_logs/cf_logs_2021-02-02T18:00:00Z~2021-02-02T18:30:00Z.json.gz`.

7. To pull logs with 50% sampling rate, 30 seconds of interval, store them in a different folder with different log filename prefix, without gzip compression and do not organize the logs into date/time folder:

	```
	$ sudo ./cf_logs_downloader.py -z YOUR_ZONE_ID -t YOUR_API_TOKEN -r 0.5 -i 30 --path /home/user/cf_logging/ --prefix example_com --no-gzip --no-organize
	```

	Expected outcome: your logs will be stored in `/home/user/cf_logging/example_com_1970-01-01T18:00:00Z~1970-01-01T18:00:30Z.json` initially. Subsequent logs will be stored in their respective folder based on date and time.

## Retrieving items in queue
1. Specifying `--list-queue` as the parameter will display the list of failed tasks waiting for retry, but without any formatting and sorting. The output will look like this:
	`
	[{"id": 99, "data": {"folder_time": "2021-04-22 13:22:00", "log_start_time_utc": "2021-04-22 05:22:00", "log_end_time_utc": "2021-04-22 05:22:30", "reason": "Logpull error (HTTP 429, Cloudflare 10000 - Rate limited. Please wait and consider throttling your request speed)"}, "timestamp": 1619069053.252742}, {"id": 100, "data": {"folder_time": "2021-04-22 13:22:30", "log_start_time_utc": "2021-04-22 05:22:30", "log_end_time_utc": "2021-04-22 05:23:00", "reason": "Logpull error (HTTP 401, Cloudflare 10000 - Authentication error)"}, "timestamp": 1619069082.8486648}, {"id": 101, "data": {"folder_time": "2021-04-22 13:21:30", "log_start_time_utc": "2021-04-22 05:21:30", "log_end_time_utc": "2021-04-22 05:22:00", "reason": "Logpull error (HTTP 401, Cloudflare 10000 - Authentication error)"}, "timestamp": 1619069105.1588771}, {"id": 102, "data": {"folder_time": "2021-04-22 13:23:00", "log_start_time_utc": "2021-04-22 05:23:00", "log_end_time_utc": "2021-04-22 05:23:30", "reason": "Logpull error (HTTP 401, Cloudflare 10000 - Authentication error)"}, "timestamp": 1619069112.8409271}]
	`
2. Specifying `--list-queue-beauty` as the parameter will display the list of failed tasks waiting for retry. This option provides formatting as well as sorting based on the log start time. The output will look like this:
	```
	[
	  {
	    "id": 101,
	    "data": {
	      "folder_time": "2021-04-22 13:21:30",
	      "log_start_time_utc": "2021-04-22 05:21:30",
	      "log_end_time_utc": "2021-04-22 05:22:00",
	      "reason": "Logpull error (HTTP 401, Cloudflare 10000 - Authentication error)"
	    },
	    "timestamp": 1619069105.1588771
	  },
	  {
	    "id": 99,
	    "data": {
	      "folder_time": "2021-04-22 13:22:00",
	      "log_start_time_utc": "2021-04-22 05:22:00",
	      "log_end_time_utc": "2021-04-22 05:22:30",
	      "reason": "Logpull error (HTTP 429, Cloudflare 10000 - Rate limited. Please wait and consider throttling your request speed)"
	    },
	    "timestamp": 1619069053.252742
	  },
	  {
	    "id": 100,
	    "data": {
	      "folder_time": "2021-04-22 13:22:30",
	      "log_start_time_utc": "2021-04-22 05:22:30",
	      "log_end_time_utc": "2021-04-22 05:23:00",
	      "reason": "Logpull error (HTTP 401, Cloudflare 10000 - Authentication error)"
	    },
	    "timestamp": 1619069082.8486648
	  },
	  {
	    "id": 102,
	    "data": {
	      "folder_time": "2021-04-22 13:23:00",
	      "log_start_time_utc": "2021-04-22 05:23:00",
	      "log_end_time_utc": "2021-04-22 05:23:30",
	      "reason": "Logpull error (HTTP 401, Cloudflare 10000 - Authentication error)"
	    },
	    "timestamp": 1619069112.8409271
	  }
	]
	```
3. Specifying `--queue-size` as the parameter will display the number of items inside the queue. Useful to know how many failed tasks pending for retry.
4. If you specify any of the parameters as listed above, all other parameters that you specified (e.g. `-z` or `-t`) will be ignored.
5. Currently it does not support separating queues based on Zone ID (domain). You may get unexpected behavior when you try to change the Zone ID while there are items in the queue, which is not bind to any Zone IDs.

## Known issues
1. None

## Notes
1. Currently only Cloudflare API Token can be used to authenticate against Cloudflare APIs. Global API key is not supported, as this is a more insecure option.
2. All the logpull activity logs will be written in `/var/log/cf_logs_downloader/` folder. Make sure you have the appropriate permission (root) to run the script.
3. Each successful logpull activity will be written in `succ.log` file.
4. If a logpull task failed, the failed task will be put inside a queue. A separate thread will keep checking the queue for new items, and reattempt the logpull process. The thread will pick up new items after each logpull activity for every 3 seconds. If there's 3 consequtive failed logpull activities, then the thread will wait for 60 seconds before performing the next logpull activity. 
5. Some logpull tasks can't be retried because of known error (for example, requesting bot management field from a zone which does not have bot management enabled). In this case, the failed logpull activity will be written in `fail.log`.
6. If you specify `--one-time` parameter, you must specify `--start-time` and `--end-time` at the same time and vice versa.
7. The `--start-date` must be no more than 7 days earlier than now (according to [Cloudflare Developers Docs](https://developers.cloudflare.com/logs/logpull-api/requesting-logs)).
8. The `--end-date` must be at least 1 minute earlier than now and later than `--start-date` (according to [Cloudflare Developers Docs](https://developers.cloudflare.com/logs/logpull-api/requesting-logs)).
9. The maximum range between `--start-time` and `--end-time` must be 1 hour only. Otherwise, Cloudflare API calls will fail (according to [Cloudflare Developers Docs](https://developers.cloudflare.com/logs/logpull-api/requesting-logs)). 
10. Only specify `--bot-management` if your zone has Bot Management enabled, otherwise Cloudflare API will return Error 1010 - auth.forbidden.
