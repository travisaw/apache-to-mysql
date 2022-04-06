# apache-to-mysql
Python script to parse apache logs to mysql

Reference Libraries
https://github.com/amandasaurus/apache-log-parser

# config.yml
File contains runtime configurations for this tool.
## Database settings
* host - database host name
* catalog - catalog or database to use
* username - database username
* password  - database password

## Other Settings
* printfirstline - Print first line of output from logfile to console. 0/1 = no/yes
* executeSql - Execute commands to write output to sql.  0/1 = no/yes
* logFormat - Apache log format. Pattern below is from the LogFormat setting in apache2.conf/httpd.conf file. You will likely need to change this value to the pattern your system uses
