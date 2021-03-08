

# cf-logs-downloader
A little tool to pull/download HTTP Access logs from Cloudflare Enterprise Log Share (ELS) and save it on local storage.

## Prerequisites
- You must have an active Cloudflare Enterprise zone in order to use Cloudflare Enterprise Log Share (ELS).
- Make sure the zone that you want to pull logs from already has Log Retention enabled - refer to [this link](https://developers.cloudflare.com/logs/logpull-api/enabling-log-retention) on how to enable it.
- Make sure your Cloudflare user account has the permission to access Zone logs (particularly, Log Share Reader role). If you are unsure about that, contact your Administrator. If you are an Administrator already, no further action is required.
- You need to [create an API Token from the Cloudflare Dashboard](https://dash.cloudflare.com/profile/api-tokens) to allow access to logs.
- Requires root access to your local machine.
- Requires Python 3 to be installed in your local machine (tested with Python 3.69, but versions above/below 3.69 should work) - [Download it from here](https://www.python.org/downloads/). 
- Python "requests" library must be installed - install it using `pip3 install requests` command.
- Currently only supports Linux. Windows isn't supported yet.

## Getting started
1. Clone this repository to your local machine - `git clone https://github.com/erictung1999/cf-logs-downloader.git`
2. Make "cf_logs_downloader.py" executable - `chmod +x cf_logs_downloader.py`
3. Verify the script is working by executing `./cf_logs_downloader.py -v`. You should see this:

	```
	Version 2.0.1
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
  -z ZONE, --zone ZONE  Specify the Cloudflare Zone ID, if CF_ZONE_ID
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
  --path PATH           Specify the path to store logs. By default, it will
                        save to /var/log/cf_logs/
  --prefix PREFIX       Specify the prefix name of the logfile being stored on
                        local storage. By default, the file name will begins
                        with cf_logs.
  --no-organize         Instruct the program to store raw logs as is, without
                        organizing them into date and time folder.
  --no-gzip             Do not compress the raw logs.
  --one-time            Only pull logs from Cloudflare for one time, without
                        scheduling capability. You must specify the start time
                        and end time of the logs to be pulled from Cloudflare.
  --start-time START_TIME
                        Specify the start time of the logs to be pulled from
                        Cloudflare. The start time is inclusive. You must
                        follow the ISO 8601 date format, in UTC timezone.
                        Example: 2020-12-31T12:34:56Z
  --end-time END_TIME   Specify the end time of the logs to be pulled from
                        Cloudflare. The end time is exclusive. You must follow
                        the ISO 8601 date format, in UTC timezone. Example:
                        2020-12-31T12:35:00Z
  --debug               Enable debugging functionality.
  -v, --version         Show program version
```

## Environment variables
Here are some environment variables that you can create while using this tool:
1. `CF_ZONE_ID` - Specify the Cloudflare Zone ID. The value of `CF_ZONE_ID` will be overwritten by `-z` or `--zone` parameter if you specify the value using one of the parameters.
2. `CF_TOKEN` - Specify the Cloudflare Access Token. The value of `CF_TOKEN` will be overwritten by `-t` or `--token` if you specify the value using one of the parameters.


## Example usage
1. At a bare minimum, you must specify Cloudflare Zone ID and Access Token while using the tool. By doing so, this tool will use default values for the below configurations:

	* Log sampling rate: 100% (represented by 1 in Cloudflare API)
	* Logpull interval: 60 seconds
	* Logpull storage path: `/var/log/cf_logs/`
	* Log filename prefix: `cf_logs`
	* Enable folder organize by date and time
	* Enable Gzip compression

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

4. To instruct the tool not to save the logs in compressed (gzip) format, and store the logs in a different folder:

	```
	$ sudo ./cf_logs_downloader.py -z YOUR_ZONE_ID -t YOUR_API_TOKEN --no-gzip --path /root/Downloads/my_cloudflare_log/
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

## Known issues
1. Cloudflare might block the tool from pulling the logs from certain zones (not all) with Browser Integrity Check security service.

## Notes
1. Currently only Cloudflare API Token can be used to authenticate against Cloudflare APIs. Global API key is not supported, as this is a more insecure option.
2. All the logpull activity logs will be written in `/var/log/cf_logs_downloader/` folder. Make sure you have the appropriate permission (root) to run the script.
3. Each successful logpull activity will be written in `succ.log` file, while each failed logpull activity will be written in `fail.log`.
4. Each logpull activity will be given 5 attempts. If the first attempt fails (due to network conditions, Cloudflare API issue, etc.), this tool will retry for another 4 times.
5. If you specify `--one-time` parameter, you must specify `--start-time` and `--end-time` at the same time and vice versa.
6. The `--start-date` must be no more than 7 days earlier than now (according to [Cloudflare Developers Docs](https://developers.cloudflare.com/logs/logpull-api/requesting-logs)).
7. The `--end-date` must be at least 1 minute earlier than now and later than `--start-date` (according to [Cloudflare Developers Docs](https://developers.cloudflare.com/logs/logpull-api/requesting-logs)).
8. The maximum range between `--start-time` and `--end-time` must be 1 hour only. Otherwise, Cloudflare API calls will fail (according to [Cloudflare Developers Docs](https://developers.cloudflare.com/logs/logpull-api/requesting-logs)). 
