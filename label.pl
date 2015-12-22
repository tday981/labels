#!/usr/bin/perl
use File::Basename;
use DBI;
use warnings;

$count=0;

##Check for command line arguments. Expects two to be present.
if ( $#ARGV <= "0" ) {

	print "This script requires a database and directory be specified at run time.\n";
	die "Ex. \"./label.pl db_name dir_name\"\n" ;

}

##Assign commandline arguments to variables
$db=$ARGV[0];
$dir=$ARGV[1];

##List files for processing
if ( $dir eq "apac" ) {

#@mainProc=("ddnCoreLabels.xml","ddnLabels.xml","ddnPublishers.xml","ddnReqLabels.xml","finLabels.xml");
@mainProc=("ddnCoreLabels.xml","ddnLabels.xml","ddnPublishers.xml","ddnReqLabels.xml");
@otherProc=("efxsitelist.xml","Funnel.xml","ddnServers.xml","RecoveryLabelExceptionList.xml");

}

else {


#@mainProc=("ddnCoreLabels.xml","ddnLabels.xml","ddnPublishers.xml","ddnReqLabels.xml","LocalLabels.xml","finLabels.xml");
#@otherProc=("efxsitelist.xml","Funnel.xml","ddnServers.xml","RecoveryLabelExceptionList.xml","finServers.xml");
@mainProc=("ddnCoreLabels.xml","ddnLabels.xml","ddnPublishers.xml","ddnReqLabels.xml","LocalLabels.xml");
@otherProc=("efxsitelist.xml","Funnel.xml","ddnServers.xml","RecoveryLabelExceptionList.xml");

}

##set up the connect to the database
#$dbh = DBI->connect("DBI:mysql:database=pre_prod;host=localhost","name","password",{'RaiseError' => 1});
$dbh = DBI->connect("DBI:mysql:database=$db;host=reghost","quest","Pegestech1",{'RaiseError' => 1});


#@list=glob("$dir/*.xml");
#@list=("$dir/ddnLabels.xml");

##loop to process each xml file
foreach $in (@mainProc) {

$comm=0;
	
	print "Processing $in...\n";
	
	##Open current file in a file handle
	open $fh, '<', $dir."/".$in or die "Can't open $in: $!\n";
	@file=<$fh>;

	#Trim file name for genric use 
	($name=basename($in)) =~ s/\.xml//;

	#sql for version,major,minor,date,port,dscp
	$prepVer = $dbh->prepare("insert into ".$name." (version) VALUES(?);");
	$prepMaj = $dbh->prepare("insert into ".$name." (major) VALUES(?);");
	$prepMin = $dbh->prepare("insert into ".$name." (minor) VALUES(?);");
	$prepInc = $dbh->prepare("insert into ".$name." (includeHref) VALUES(?);");
	$prepDate = $dbh->prepare("insert into ".$name." (date) VALUES(?);");
	$prepPort = $dbh->prepare("insert into ".$name." (portId,portNum) VALUES(?,?);");
	$prepDscp = $dbh->prepare("insert into ".$name." (dscpId,dscpNum) VALUES(?,?);");
	$prepConst = $dbh->prepare("insert into ".$name." (conId,conNum) VALUES(?,?);");


	##Define sql statements
	if ( $name eq "LocalLabels" ) {

		$prepConf = $dbh->prepare("insert into ".$name." (comment,labelId,source,destination,multTag,consumer,provider,type,SrcID,constituents,sps,instSide,instID) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);");

	}

	elsif ( $name eq "ddnReqLabels" || $name eq "ddnPublishers" ) {

		#$prepConf->execute($comment,$l,$sour,$des,$mtag,$consumer,$spsL,$prov,$type,$srcid,$cons,$cvaM,$sps,$instLet,$inid);
		$prepConf = $dbh->prepare("insert into ".$name." (comment,labelId,source,destination,multTag,consumer,spsList,provider,type,SrcID,constituents,cvaMultTag,sps,instSide,instID) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");

	}
	
	else {

		$prepConf = $dbh->prepare("insert into ".$name." (comment,labelId,source,destination,multTag,consumer,spsList,provider,type,SrcID,constituents,sps,instSide,instID) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?);");

	}
		
	#Define sql statement for multicast tables
	$prepMult = $dbh->prepare("insert into ".$name." (tag,addr,port,DSCP) VALUES(?,?,?,?);");

		#Loop to process each line in the current file
		foreach $line (@file) {

			#Trip variable to start collecting comments on labels
			if ( $line =~ /\Q<labelTable\E/ ) {

				$comm=1;

			}

			#sub routines for collecting header info
			version();major();minor();date();include();port();dscp();const();

			if ( $line =~ /\Q<item site1\E/ ) {

				@itList=split(/\"/,$line);
				$prepConf->execute($itList[1],$itList[3],$itList[5],$itList[7]);

			}

			#Parse contents of multicast info and insert to db
			if ( $line =~ /\Q<multAddr TAG\E/ ) {

				@multAddr=split(/\"/,$line);
				$prepMult->execute($multAddr[1],$multAddr[3],$multAddr[5],$multAddr[7]);
				#push (@marray,$mAddr);

			}

			if ( $line =~ /<!-- [A-Za-z0-9]/ && $comm == "1" ) {

				chomp $line;
				$comment=$line;

			}

			#Collect label id for later use
			if ( $line =~ /\Q<label ID\E/ ){

				@lar=split(/\"/,$line);
				$l=$lar[1];
				#print "label is $l\n";
			}

			#Collect source for later use
			if ( $line =~ /\Q<source\E/ ){

				@sar=split(/[<,>]+/,$line);
				$sour=$sar[2];
				#print "source is $sour\n";

			}

			#Collect destination for later use
			if ( $line =~ /\Q<destination\E/ ){

				@dest=split(/[<,>]+/,$line);
				$des=$dest[2];
				#print "destination is $des\n";

			}

			#Collect multicast tag for later use
			if ( $line =~ /\Q<multTag\E/ ){

				@multTag=split(/[<,>]+/,$line);
				$mtag=$multTag[2];
				#print "multTag is $mtag\n";

			}

			#Collect consumer for later use
			if ( $line =~ /\Q<consumer\E/ ){

				@consum=split(/[<>]+/,$line);
				$consumer=$consum[2];
				#print "consumer is $cons\n";

			}

			#Collect sps list for later use
			if ( $line =~ /\Q<spsList\E/ ){

				if ( $name eq "LocalLabels" ) {

					next;

				}

				@spsList=split(/[<,>]+/,$line);
				$spsL=$spsList[2];
				#print "spsList is $spsL\n";

			}

			#Collect provider name for later use
			if ( $line =~ /\Q<provider NAME\E/ ){

				@provid=split(/\"/,$line);
				$prov=$provid[1];
				#print "provider is $prov\n";

			}

			#Collect type for later use
			if ( $line =~ /\Q<type\E/ ){

				@ty=split(/[<,>]+/,$line);
				$type=$ty[2];
				#print "type is $type\n";

			}

			#Collect source id for later use
			if ( $line =~ /\Q<SrcID\E/ ){

				@sid=split(/[<,>]+/,$line);
				$srcid=$sid[2];
				#print "SrcID is $srcid\n";

			}

			#Collect contituent for later use
			if ( defined $l && $line =~ /\Q<constituents\E/ ){

				@constit=split(/[<,>]+/,$line);
				$cons=$constit[2];
				#print "constituents is $cons\n";

			}

			#Collect cva multicast tag for later use
			if ( $name eq "ddnReqLabels" || $name eq "ddnPublishers" ) {

				if ( $line =~ /\Q<cvaMultTag\E/ ) {

					@cva=split(/[<>]+/,$line);
					$cvaM=$cva[2];	

				}

			}

			#Collect sps ric for later use
			if ( $line =~ /\Q<sps>\E/ ){

				@Sps=split(/[<,>]+/,$line);
				$sps=$Sps[2];
				#print "sps is $sps\n";

			}

			#Collect the letter designation and name of hosts for later use
			if ( $line =~ /\Q<inst ID\E/ ) {

				@instid=split(/[<,>,"]+/,$line);
				$instLet=$instid[2];
				$inid=$instid[3];
				#print "instId is $inid with letter $insLet\n";
				#$conf = "$l/$sour/$des/$mtag/$consumer/$spsL/$prov/$type/$srcid/$cons/$sps/$insLet/$inid";

				#Insert relevant data based on files being processed
				if ( $name eq "ddnReqLabels" || $name eq "ddnPublishers" ) {

					#Will insert labelid,source,destination,multTag,consumer,spslist,provider,type,srcid,constituents,cvamult,sps,host letter,host name
					$prepConf->execute($comment,$l,$sour,$des,$mtag,$consumer,$spsL,$prov,$type,$srcid,$cons,$cvaM,$sps,$instLet,$inid);

				}

				elsif ( $name eq "LocalLabels" ) {

					#Will insert labelid,source,destination,multTag,consumer,provider,type,srcid,constituents,sps,host letter,host name
					$prepConf->execute($comment,$l,$sour,$des,$mtag,$consumer,$prov,$type,$srcid,$cons,$sps,$instLet,$inid);

				}

				else {
		
					#Will insert labelid,source,destination,multTag,consumer,spslist,provider,type,srcid,constituents,sps,host letter,host name
					$prepConf->execute($comment,$l,$sour,$des,$mtag,$consumer,$spsL,$prov,$type,$srcid,$cons,$sps,$instLet,$inid);

				}

				#push (@entry,$conf);

			}

			#Undefine variables once provider close tag is read
			if ( $line =~ /\Qprovider>\E/ ){

				undef $prov;
				undef $type;
				undef $srcid;
				undef $cons;
				undef $sps;
				undef $instLet;
				undef $inid;
				undef $cvaM;

			}

			#Undefine variables once label close tag is read
			if ( $line =~ /\Qlabel>\E/ ){

				undef $l;
				undef $sour;
				undef $des;
				undef $mtag;
				undef $consumer;
				undef $spsL;
				undef $comment;
				#print "Matched label close\n";

			}

		}

}

foreach $in (@otherProc) {

	print "Processing $in...\n";
	
	##Open current file in a file handle
	open $fh, '<', $dir."/".$in or die "Can't open input $!\n";
	@file=<$fh>;

	#Trim file name for genric use 
	($name=basename($in)) =~ s/\.xml//;

	#sql for version,major,minor,date,port,dscp
	$prepVer = $dbh->prepare("insert into ".$name." (version) VALUES(?);");
	$prepMaj = $dbh->prepare("insert into ".$name." (major) VALUES(?);");
	$prepMin = $dbh->prepare("insert into ".$name." (minor) VALUES(?);");
	$prepInc = $dbh->prepare("insert into ".$name." (includeHref) VALUES(?);");
	$prepDate = $dbh->prepare("insert into ".$name." (date) VALUES(?);");
	$prepPort = $dbh->prepare("insert into ".$name." (portId,portNum) VALUES(?,?);");
	$prepDscp = $dbh->prepare("insert into ".$name." (dscpId,dscpNum) VALUES(?,?);");
	$prepConst = $dbh->prepare("insert into ".$name." (conId,conNum) VALUES(?,?);");

	##Define sql statements
	if ( $name eq "Funnel" ) {

		$prepMultGroup = $dbh->prepare("insert into ".$name." (multGroup,labelId) VALUES(?,?);");
		$prepPair = $dbh->prepare("insert into ".$name." (pairName) VALUES(?);");
		$prepPairFun = $dbh->prepare("insert into ".$name." (pairName,funnelName) VALUES(?,?);");
		$prepInput = $dbh->prepare("insert into ".$name." (funnelName,outGroup,inputLabel,instId,instName) VALUES(?,?,?,?,?);");
		$prepOutGroup = $dbh->prepare("insert into ".$name." (outGroup) VALUES(?);");

		foreach $line (@file) {

			#sub routines for collecting header info
			version();major();minor();date();include();port();dscp();const();
	
			if ( $line =~ /\Q<multGroup\E/ ) {
	
				@mgroup=split(/\"/,$line);
				$multGroup=$mgroup[1];
	
			}
	
			if ( $line =~ /\Q<label id\E/ ) {
	
				@lid=split(/\"/,$line);
				$labId=$lid[1];
				$prepMultGroup->execute($multGroup,$labId);
				#print "$multGroup/$labId\n";
	
			}
	
			if ( $line =~ /\Q<Pair name\E/ ) {
	
				@pName=split(/\"/,$line);
				$pairn=$pName[1];
				$prepPair->execute($pairn);
	
			}
	
			if ( $line =~ /\Q<Funnel name\E/ ) {
	
				@fName=split(/\"/,$line);
				$funName=$fName[1];
				#$funIn=$fName[3];
				$prepPairFun->execute($pairn,$funName);
	
				#if ( defined $pairn && defined $funName ) {
	
			}
	
			if ( $line =~ /\Q<\/Pair\E/ ) {
	
				undef $pairn;
				undef $funName;

			}
	
			if ( $line =~ /\Q<Output Group\E/ ) {
	
				@oGroup=split(/\"/,$line);
				$outG=$oGroup[1];
	
			}
	
			if ( $line =~ /\Q<InputLabel ID\E/ ) {
	
				@iLab=split(/\"/,$line);
				$Ilab=$iLab[1];
				$count++;
	
			}
	
			if ( $line =~ /\Q<inst ID\E/ ) {
	
				@inId=split(/\"/,$line);
				$instId=$inId[1];
				$instName=$inId[3];
				$count++;
	
				if ( defined $Ilab && defined $funName && defined $instId ) {
	
				$prepInput->execute($funName,$outG,$Ilab,$instId,$instName);
				#	print "$Ilab/$funName/$instId/$instName\n";	
	
				}
	
			}

			if ( defined $outG && $count == "0" ) {

				$prepOutGroup->execute($outG);
				$count++;

			}
	
			if ( $line =~ /\Q<\/Funnel\E/ ) {
	
			#	print "matched\n";
				undef $funName;
				undef $instId;
	
			}
	
			if ( $line =~ /\Q<\/InputLabel\E/ ) {
	
			#	print "matched\n";
				undef $funName;
				undef $instId;
				undef $Ilab;
	
			}

			if ( $line =~ /\Q<\/Output\E/ ) {

				undef $outG;
				$count=0;

			}
	
		}
	}
	
	elsif ( $name eq "efxsitelist" ) {

		$prepConf = $dbh->prepare("insert into ".$name." (item1,item2,item3,item4) VALUES(?,?,?,?);");

		foreach $line (@file) {
			version();major();minor();date();include();port();dscp();const();

			if ( $line =~ /\Q<item site1\E/ ) {

				@itList=split(/\"/,$line);
				$prepConf->execute($itList[1],$itList[3],$itList[5],$itList[7]);

			}

		}


	}
	
	elsif ( $name eq "ddnServers" ) {

		$prepConf = $dbh->prepare("insert into ".$name." (comment,servName,ddn1,ddn2) VALUES(?,?,?,?);");
		$prepOne = $dbh->prepare("insert into ".$name." (comment) VALUES(?);");

		foreach $line (@file) {

			version();major();minor();date();include();port();dscp();const();

			#Trip variable to start collecting comments on labels

			if ( $line =~ /<!-- [A-Za-z0-9]/ ) {

				chomp $line;
				$comment=$line;

				#print "comment is $comment\n";

				if ( $comment =~ /\Q<!-- HTC - IDN & Aurora MTC  -->\E/ ) {

					#print "matched comment\n";
					$prepOne->execute($comment);

				}

			}

			if ( $line =~ /\Q<server NAME\E/ ) {

				@sName=split(/\"/,$line);

				$prepConf->execute($comment,$sName[1],$sName[3],$sName[5]);

			}
		}

	}
	
	elsif ( $name eq "RecoveryLabelExceptionList" ) {

		$prepConf = $dbh->prepare("insert into ".$name." (labelId) VALUES(?);");

		foreach $line (@file) {

			version();major();minor();date();include();port();dscp();const();
			if ( $line =~ /\Q<label ID\E/ ) {

				@labelId=split(/\"/,$line);
				$prepConf->execute($labelId[1]);

			}

		}

	}

	elsif ( $name eq "finServers" ) {

		foreach $line (@file) {

			version();major();minor();date();

		}

	}
}

print "\nDone\n";



#foreach $line (@in) {

	#version();major();minor();date();include();port();dscp();


#}

sub version { 

	if ( $line =~ /\Q<version TEXT\E/ ) {

		@vers=split(/\"/,$line);
		$prepVer->execute($vers[1]);
		#print "version is $ver\n";

	}

}

sub major { 

	if ( $name eq "Funnel" ) {

		if ( $line =~ /\QMajor=\E/ ) {

			@ma=split(/\"/,$line);
			$prepMaj->execute($ma[3]);
			#print "major is $ma\n";

		}

	}

	else {

		if ( $line =~ /\Q<major\E/ ) {

			@ma=split(/[<>]/,$line);
			$prepMaj->execute($ma[2]);
			#print "major is $ma\n";

		}
	
	}

}

sub minor { 

	if ( $name eq "Funnel" ) {

		if ( $line =~ /\QMinor=\E/ ) {

			@mi=split(/\"/,$line);
			$prepMin->execute($mi[5]);
			#print "minor is $min\n";

		}

	}

	else {

		if ( $line =~ /\Q<minor\E/ ) {

			@mi=split(/[<>]/,$line);
			$prepMin->execute($mi[2]);
			#print "minor is $min\n";

		}

	}

}

sub date { 

	if ( $line =~ /\Q<date\E/ ) {

		@dat=split(/[<>]/,$line);
		$prepDate->execute($dat[2]);
		#print "date is $date\n";

	}

}

sub include { 

	if ( $line =~ /\Q<xi:include\E/ ) {

		@include=split(/\"/,$line);
		#print "include is $include[1]\n";
		$prepInc->execute($include[1]);

	}

}

sub port { 

	if ( $line =~ /\Q<port ID\E/ ) {

		@por=split(/[<">]/,$line);
		$prepPort->execute($por[2],$por[4]);
		#print "port name is $portName and port number is $portNum\n";

	}

}

sub dscp { 

	if ( $line =~ /\Q<dscp ID\E/ ) {

		@dsc=split(/[<">]/,$line);
		$prepDscp->execute($dsc[2],$dsc[4]);
		#print "dscp name is $dscpName and dscp number is $dscpNum\n";

	}

}

sub const { 

	if ( $line =~ /\Q<constituent ID\E/ ) {

		@const=split(/[<">]/,$line);
		$prepConst->execute($const[2],$const[4]);
		#print "dscp name is $dscpName and dscp number is $dscpNum\n";

	}

}
