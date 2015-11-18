#!/usr/bin/perl
use File::Basename;
use DBI;
#use warnings;

##Check for command line arguments. Expects two to be present.
if ( $#ARGV <= "0" ) {

	print "This script requires a database and directory be specified at run time.\n";
	die "Ex. \"./label.pl db_name dir_name\"\n" ;

}

##Assign commandline arguments to variables
$db=$ARGV[0];
$dir=$ARGV[1];

##set up the connect to the database
#$dbh = DBI->connect("DBI:mysql:database=pre_prod;host=localhost","name","password",{'RaiseError' => 1});

questions();

#print "table is $table and var is $var\n";
#

sub questions {

	print "Which table would you like to modify?\n";
	$table=<STDIN>;
	chomp $table;

	print "\nWhat would you like to modify?\n";
	$var=<STDIN>;
	chomp $var;	

	if ( $var =~ /provider/ ) {

		print "\nChange provider name?\n";
		$pnq=<STDIN>;
		chomp $pnq;

		if ( $pnq =~ /y/ ) {

			print "\nFor which label?\n";
			$labId=<STDIN>;
			chomp $labId;

			print "\nOld name:\n";
			$oln=<STDIN>;
			chomp $oln;

			print "\nNew name:\n";
			$nen=<STDIN>;
			chomp $nen;

		}

		print "change label $labId provider from $oln to $nen for table $table in $db\n";
	}
			

	return ($table,$var,$labId,$oln,$nen);
	
}
