#!/usr/bin/perl
use File::Basename;
use DBI;
use warnings;

@tables=('ddnCoreLabels','ddnLabels','ddnPublishers','ddnReqLabels','ddnReqLabels','LocalLabels',"finLabels","finServers","efxsitelist","Funnel","ddnServers","RecoveryLabelExceptionList");

if ( $#ARGV == "-1" ) {

        print "This script requires a database be specified.\n";
        die "Ex. \"./label.pl db_name\"\n" ;

}

$db=$ARGV[0];

#$dbh = DBI->connect("DBI:mysql:database=pre_prod;host=localhost","name","password",{'RaiseError' => 1});
$dbh = DBI->connect("DBI:mysql:database=$db;host=reghost","quest","Pegestech1",{'RaiseError' => 1});
#$prepConf = $dbh->prepare("truncate table ?;");

#$dbh->trace(5);


foreach $tab (@tables) {

	print "Processing $tab...\n";
	$dbh->do("truncate table $tab;");

}

print "\nDone\n";
