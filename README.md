[![LICENSE](http://img.shields.io/:license-apache-brightgreen.svg)](https://github.com/devoshm/MySQLInstaBack/blob/master/LICENSE)

# MySQLInstaBack

It creates a replication between MySQL and Cassandra to have instant backup of all MySQL data.

**How normal backup works?**<br>
In MySQL, there is a command, “mysqldump –h [hostIP] –P port –u [username] -p [password] [db_name] –single-transaction --quick > [filename.sql]”.<br/>
This command will be run periodically in all the MySQL clusters from a remote machine where all the back ups will be stored.

**Problems in Existing Backup**
<ul>
  <li>Mysqldump command just takes a snapshot of the entire DB at the time of running.</li>
  <li>Duplicate copy of each back up.</li>
  <li>Necessity of increased storage space.</li>
  <li>Restore can be done only to particular date of backup.</li>
  <li>Not anytime restore is possible.</li>
</ul>

**How this works?**
<ul>
  <li>MySQL writes all the commands to binlog file through which the data is replicated to the slaves.</li>
  <li>We are tapping into MySQL replication stream and store all the command being executed in Master in Cassandra in the same order of execution along with timestamp.</li>
  <li>Now, given any time, we will be able to restore to that point by sequentially executing all the commands.</li>
  <li>Also, it doesn’t occupy much space as we store only the command and not the actual data. Plus, there is no duplicate.</li>
</ul>
