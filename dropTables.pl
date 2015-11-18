#!/usr/bin/perl
use File::Basename;
use DBI;
use warnings;

@tables=('ddnCoreLabels','ddnLabels','ddnPublishers','ddnReqLabels','LocalLabels','finLabels','finServers','efxsitelist','ddnServers','RecoveryLabelExceptionList','Funnel');


if ( $#ARGV == "-1" ) {

        print "This script requires a database be specified at run time.\n";
        die "Ex. \"./dropTables.pl db_name\"\n" ;

}

$db=$ARGV[0];
#$dbh = DBI->connect("DBI:mysql:database=pre_prod;host=localhost","name","password",{'RaiseError' => 1});
#
#$prepConf = $dbh->prepare("truncate table ?;");

#$dbh->trace(5);


foreach $tab (@tables) {

	$dbh->do("drop table $tab;");
	print "Dropped $tab...\n";

}
