#!/usr/bin/perl
use File::Basename;
use DBI;
use List::MoreUtils qw(uniq);
#use warnings;
#$table="ddnReqLabels";
#$table="ddnCoreLabels";
#@mainProc=("RecoveryLabelExceptionList");
@mainProc=("ddnCoreLabels","ddnLabels","ddnPublishers","ddnReqLabels","ddnServers","efxsitelist","finLabels","finServers","Funnel","LocalLabels","RecoveryLabelExceptionList");

if ( $#ARGV == "-1" ) {

	print "This script requires a database to be specified at run time.\n";
	die "Ex. \"./select.pl db_name\"\n" ;

}

if ( ! -d "out" ) {

	mkdir "out";

}

foreach $table (@mainProc) {

$file="out/".$table.".xml";

print "processing $file\n";

open $out, '>',$file or die "Can't open out $!\n";

#$inc=0;

#define whitespace values for xml tag
$ws6=(" " x 4);  #version,date,servers,multAddrs,constituents,labelTable
$ws1=(" " x 8);  #version,date,servers,multAddrs,constituents,labelTable
$ws2=(" " x 13); #multAddr,label
$ws7=(" " x 14); #multAddr,label
$ws3=(" " x 16); #major,minor,xi,port,dscp,constituent
$ws4=(" " x 17); #source,destination,multTag,consumer,spsList,provider
$ws8=(" " x 18); #source,destination,multTag,consumer,spsList,provider
$ws5=(" " x 21); #type,SrcID,constituents,sps,inst
$ws9=(" " x 25); #type,SrcID,constituents,sps,inst

##Check for command line arguments. Expects two to be present.
$db=$ARGV[0];

##set up the connect to the database
#$dbh = DBI->connect("DBI:mysql:database=pre_prod;host=localhost","name","password",{'RaiseError' => 1});
#

	if ( $table eq "ddnServers" ) {

		Head();servName();

	}

	elsif ( $table eq "efxsitelist" ) {

		Head();efxSite();

	}

	elsif ( $table eq "finServers" ) {

		Head();finServ();

	}

	elsif ( $table eq "Funnel" ) {

		Head();outGroup();pairTable();funnel();

	}

	elsif ( $table eq "RecoveryLabelExceptionList" ) {

		Head();labelX();

	}

	else {

		Head();portId();dscpId();multAddr();constId();labelUniq();

	}

}

sub Head {

$prepVer = $dbh->prepare("select distinct version from ".$db.".".$table." where version is not null;");
$prepMaj = $dbh->prepare("select distinct major from ".$db.".".$table." where major is not null;");
$prepMin = $dbh->prepare("select distinct minor from ".$db.".".$table." where minor is not null;");
$prepDate = $dbh->prepare("select distinct date from ".$db.".".$table." where date is not null;");
$prepInc = $dbh->prepare("select distinct includeHref from ".$db.".".$table." where includeHref is not null;");


$prepVer->execute();
$prepMaj->execute();
$prepMin->execute();
$prepDate->execute();

$ver=$prepVer->fetchrow_array();
$maj=$prepMaj->fetchrow_array();
$min=$prepMin->fetchrow_array();
$date=$prepDate->fetchrow_array();
	
	if ( $table eq "ddnServers" ) {

		print $out "\<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?\>\n\n";
		print $out "\<ddn\>\n";
		print $out $ws1."\<version TEXT=\"$ver\"\>\n";
		print $out $ws3."\<major\>$maj\</major\>\n";
		print $out $ws3."\<minor\>$min\</minor\>\n";
		print $out $ws1."\</version\>\n";
		print $out $ws1."\<date\>$date\</date\>\n\n";
		print $out $ws1."\<!-- Available Resources. Each server with DDN addresses -->\n";
		print $out $ws1."\<servers>\n";

	}

	elsif ( $table eq "efxsitelist" ) {

		print $out "\<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?\>\n\n";
		print $out "\<document case=\"lower\" version=\"1.0\"\>\n";
		print $out $ws6."\<ddn\>\n";
		print $out $ws1."\<version TEXT=\"$ver\"\>\n";
		print $out $ws3."\<major\>$maj\</major\>\n";
		print $out $ws3."\<minor\>$min\</minor\>\n";
		print $out $ws1."\</version\>\n";
		print $out $ws1."\<date\>$date\</date\>\n\n";

	}

	elsif ( $table eq "finServers" || $table eq "RecoveryLabelExceptionList" ) {

		print $out "\<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?\>\n\n";
		print $out "\<ddn\>\n";
		print $out $ws1."\<version TEXT=\"$ver\"\>\n";
		print $out $ws3."\<major\>$maj\</major\>\n";
		print $out $ws3."\<minor\>$min\</minor\>\n";
		print $out $ws1."\</version\>\n";
		print $out $ws1."\<date\>$date\</date\>\n\n";

	}

	elsif ( $table eq "Funnel" ) {

		$prepInc->execute();
		$num=@inc;

		print $out "\<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?\>\n\n";
		print $out "\<FunnelLabels\>\n";
		print $out $ws1."\<version TEXT=\"$ver\" Major=\"$maj\" Minor=\"$min\" />\n";
		print $out $ws1."\<date\>$date\</date\>\n\n";
		print $out $ws1."<!-- Available Resources. Each server with DDN addresses -->\n";
		print $out $ws1."<servers>\n";
	
		if ( defined $num ) {

			while (@inc=$prepInc->fetchrow_array()) {

				print $out $ws3."<xi:include href=\"$inc[0]\"/>\n";
	
			}
		}

		print $out $ws1."</servers>\n\n";

	}

	else {

		$prepInc->execute();
		#$inc=$prepInc->fetchrow_array();
		$num=@inc;

		print $out "\<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?\>\n\n";
		print $out "\<ddn\>\n";
		print $out $ws1."\<version TEXT=\"$ver\"\>\n";
		print $out $ws3."\<major\>$maj\</major\>\n";
		print $out $ws3."\<minor\>$min\</minor\>\n";
		print $out $ws1."\</version\>\n";
		print $out $ws1."\<date\>$date\</date\>\n\n";
		print $out $ws1."\<!-- Available Resources. Each server with DDN addresses --\>\n";
		print $out $ws1."\<servers>\n";

		if ( defined $num ) {

			while (@inc=$prepInc->fetchrow_array()) {

				print $out $ws3."<xi:include href=\"$inc[0]\"/>\n";
	
			}
		}

		#print $out $ws3."\<xi:include href=\"$Inc\"/>\n";
		print $out $ws1."\</servers>\n\n";
		print $out $ws1."\<multAddrs>\n";

	}

}

sub portId {

	print $out $ws3."\<!-- Port number enumeration -->\n";

	$prepPort = $dbh->prepare("select distinct portId,portNum from ".$db.".".$table." where portId is not null;");
	$prepPort->execute();

	while ( @row = $prepPort->fetchrow_array()) {

		print $out $ws3."\<port ID=\"$row[0]\">$row[1]\<\/port>\n";

	}

	print $out "\n";

}

sub dscpId {

	print $out $ws3."\<!-- DSCP number enumeration -->\n";
	
	$prepDscp = $dbh->prepare("select distinct dscpId,dscpNum from ".$db.".".$table." where dscpId is not null;");
	$prepDscp->execute();


	while (@row = $prepDscp->fetchrow_array()) {

		print $out $ws3."\<dscp ID=\"$row[0]\">$row[1]\</dscp>\n";

	}

	print $out "\n";

}

sub multAddr {
	
	$prepMultAddr = $dbh->prepare("select distinct tag,addr,port,DSCP from ".$db.".".$table." where tag is not null;");
	$prepMultAddr->execute();

	while (@row = $prepMultAddr->fetchrow_array()) {

		print $out $ws2."\<multAddr TAG=\"$row[0]\" ADDR=\"$row[1]\" PORT=\"$row[2]\" DSCP=\"$row[3]\">\</multAddr>\n";

	}

	print $out $ws1."\</multAddrs>\n";

}

sub constId {
	
	$prepConstId = $dbh->prepare("select distinct conId,conNum from ".$db.".".$table." where conId is not null;");
	$prepConstId->execute();

	print $out "\<!-- constituents to be used within this file -->\n";
	print $out $ws1."\<constituents>\n";

	while (@row = $prepConstId->fetchrow_array()) {

		print $out $ws3."\<constituent ID=\"$row[0]\">$row[1]\</constituent>\n";

	}

	print $out $ws1."\</constituents>\n";

}

sub labelUniq {

print $out "<!-- Main label definitions -->\n";
print $out $ws1."<labelTable>\n";

if ( $table ne "finLabels" ) {

$prepFull = $dbh->prepare("select distinct comment,labelId,source,destination,multTag,consumer,spsList from ".$db.".".$table." where labelId is not null order by labelId + 0 asc;");
$prepFull->execute();

$label_uniq = $prepFull->fetchall_arrayref();

foreach $row (@$label_uniq) {

	($comment,$label,$source,$destination,$multTag,$consumer,$spsList,$provider) = @$row;
	#print $out "$label,$source,$destination,$multTag,$consumer,$spsList\n";
	print $out "<!-- -->\n";

	if ( $table ne "ddnPublishers" ) {

		print $out "$comment\n";

	}

	print $out $ws2."\<label ID=\"$label\">\n";
	print $out $ws4."<source>$source</source>\n";
	print $out $ws4."<destination>$destination</destination>\n";
	print $out $ws4."<multTag>$multTag</multTag>\n";
	print $out $ws4."<consumer>$consumer</consumer>\n";

	if ( $table ne "LocalLabels" ) {

		print $out $ws4."<spsList>$spsList</spsList>\n";

	}

	provider($label);

		$provInd=0;
		$provVal=0;

		while ($prov[$provInd]) {

			#print $out "num is $num\n";
			print $out $ws4."<provider NAME=\"$prov[$provInd][$provVal]\">\n";
			$instProv=$prov[$provInd][$provVal];
			$provVal++;
			print $out $ws5."<type>$prov[$provInd][$provVal]</type>\n";
			$provVal++;
			print $out $ws5."<SrcID>$prov[$provInd][$provVal]</SrcID>\n";
			$provVal++;
			print $out $ws5."<constituents>$prov[$provInd][$provVal]</constituents>\n";
			$provVal++;

			if ( $table eq "ddnReqLabels" || $table eq "ddnPublishers" ) {

				if ( defined $prov[$provInd][$provVal] ) {

					print $out $ws5."<cvaMultTag>$prov[$provInd][$provVal]</cvaMultTag>\n";
					$provVal++;
					print $out $ws5."<sps>$prov[$provInd][$provVal]</sps>\n"; 

				}

				else {

					$provVal++;
					print $out $ws5."<sps>$prov[$provInd][$provVal]</sps>\n"; 
					$provVal++;

				}

			}

			else {

				print $out $ws5."<sps>$prov[$provInd][$provVal]</sps>\n"; 
				
			}
		
			test($label,$instProv);
			$instInd=0;
			$instVal=0;

				while ($inst[$instInd]) {

					#print $out "instInd is $instInd and instVal is $instVal\n";
					#print $out "inst $inst[$instInd][0] and $inst[$instInd][1]\n";
					print $out $ws5."<inst ID=\"$inst[$instInd][0]\">$inst[$instInd][1]</inst>\n";

					$instInd++;
					#$instVal++;

				}	
				undef @inst;

				print $out $ws4."</provider>\n";

			$provInd++;
			$provVal=0;

		}

	undef @prov;
	print $out $ws2."</label>\n";


	}

print $out $ws1."</labelTable>\n";
print $out "\</ddn\>\n";

}

else {

	print $out $ws1."</labelTable>\n";
	print $out "\</ddn\>\n";

}
	
}

sub provider {

	$label=shift @_;
	#print $out "label is $label\n";

	if ( $table eq "ddnReqLabels" || $table eq "ddnPublishers" ) {

		$prepFull = $dbh->prepare("select distinct provider,type,SrcID,constituents,cvaMultTag,sps from ".$db.".".$table." where labelId = $label;");

	}

	else {	

		$prepFull = $dbh->prepare("select distinct provider,type,SrcID,constituents,sps from ".$db.".".$table." where labelId = $label;");

	}
		
	$prepFull->execute();
	$prov= $prepFull->fetchall_arrayref();

	$num=@$prov;
	#print $out "prov has $num\n";
	
	$val=1;
	foreach $row (@$prov) {

		if ( $table eq "ddnReqLabels" || $table eq "ddnPublishers" ) {

		($provider,$type,$SrcID,$constituents,$cvaMultTag,$sps) = @$row;
		$temp= [$provider,$type,$SrcID,$constituents,$cvaMultTag,$sps];
		push (@prov,$temp);

		}

		else {

		($provider,$type,$SrcID,$constituents,$sps) = @$row;
		$temp= [$provider,$type,$SrcID,$constituents,$sps];
		push (@prov,$temp);
	
		}

	}

	#return ($num,$provider,$type,$SrcID,$constituents,$sps);
	return ($num,@prov);

}

sub test {

	($label,$prov) = @_;
	#print $out "label is $label and provider is $prov\n";

	$prepFull = $dbh->prepare("select distinct instSide,instID from ".$db.".".$table." where labelId = \"$label\" and provider = \"$prov\";");
	$prepFull->execute();

	$inst= $prepFull->fetchall_arrayref();

	$num=@$inst;
	#print $out "inst has $num\n";
	
	foreach $row (@$inst) {

		($instSide,$instID) = @$row;

		#print $out "instSide is $instSide and instID is $instID\n";
		$temp= [$instSide,$instID];
		push (@inst,$temp);

	}

	#return ($num,$provider,$type,$SrcID,$constituents,$sps);
	return ($numInst,@inst);
	undef @inst;


}

sub servName {
	
#	$prepServName = $dbh->prepare("select distinct comment,servName,ddn1,ddn2 from ".$db.".".$table." where servName is not null;");
	$prepServComm = $dbh->prepare("select distinct comment from ".$db.".".$table." where comment is not null;");
	#$prepServName->execute();
	$prepServComm->execute();
	
	while ($comm= $prepServComm->fetchrow_array()) {

		push @clist,$comm;

	}

	foreach $com (@clist) {

		$prepServName = $dbh->prepare("select distinct servName,ddn1,ddn2 from ".$db.".".$table." where comment = \"".$com."\" and servName is not null;");
		$prepServName->execute();

		print $out "<!--  -->\n";
		
		if ( $com =~ /IDN\s&/ ) {

			print $out "$com\n";
			next;

		}

		print $out "$com\n";

		while (@row = $prepServName->fetchrow_array()) {

			print $out $ws2."\<server NAME=\"$row[0]\" DDN1=\"$row[1]\" DDN2=\"$row[2]\">\</server>\n";

		}
	}
	
	print $out $ws1."</servers>\n";
	print $out "</ddn>\n";
}

sub efxSite {

	$prepEfx = $dbh->prepare("select distinct item1,item2,item3,item4 from ".$db.".".$table." where item1 is not null;");
	$prepEfx->execute();	
	
	print $out $ws1."<!-- EFX pairings for TMS usage - all primary - secondary and tertiary EFX PoPs -->\n";
	print $out $ws1."<livepartnersitepairs>\n";
	
	while (@row = $prepEfx->fetchrow_array()) {

		if ( defined $row[2] && defined $row[3] ) {

			print $out $ws2."\<item site1=\"$row[0]\" site2=\"$row[1]\" site3=\"$row[2]\" site4=\"$row[3]\"/>\n"
		}

		else {

			print $out $ws2."\<item site1=\"$row[0]\" site2=\"$row[1]\"/>\n"
		
		}

	}

	print $out $ws1."</livepartnersitepairs>\n";
	print $out $ws6."</ddn>\n";
	print $out "</document>\n";

}

sub finServ {

	print $out $ws1."<!-- Available Resources. Each server with DDN addresses -->\n";
	print $out $ws1."<servers>\n";
	print $out $ws1."</servers>\n";
	print $out "</ddn>\n";

}

sub outGroup {

print $out $ws1."<outGroup>\n";

$prepMultGroup = $dbh->prepare("select distinct multGroup from ".$db.".".$table." where multGroup is not null");
$prepMultGroup->execute();

$multUniq = $prepMultGroup->fetchall_arrayref();

foreach $row (@$multUniq) {

	($multGroup) = @$row;

	$prepLabelId = $dbh->prepare("select distinct labelId from ".$db.".".$table." where multGroup=\"$multGroup\"");
	$prepLabelId->execute();

	print $out $ws7."<multGroup id=\"$multGroup\">\n";

	while (@lab = $prepLabelId->fetchrow_array()) {

		print $out $ws8."<label id=\"$lab[0]\"/>\n";

	}

	print $out $ws7."</multGroup>\n";

}

print $out $ws1."</outGroup>\n\n";
	
}

sub pairTable {

print $out "<!-- Main definitions - Funnel device pairs constituent Funnel names -->\n";
print $out $ws1."<pairTable>\n";

$prepPairName = $dbh->prepare("select distinct pairName from ".$db.".".$table." where  pairName is not null");
$prepPairName->execute();

$pair = $prepPairName->fetchall_arrayref();

foreach $row (@$pair) {

	($pairName) = @$row;

	$prepFunName = $dbh->prepare("select distinct funnelName from ".$db.".".$table." where pairName=\"$pairName\"");
	$prepFunName->execute();

	print $out $ws7."<Pair name=\"$pairName\">\n";

	while (@fun = $prepFunName->fetchrow_array()) {

		if ( !defined $fun[0] ) {

			next;

		} 

		else {

		print $out $ws8."<Funnel name=\"$fun[0]\"/>\n";

		}

	}

	print $out $ws7."</Pair>\n";

}

print $out $ws1."</pairTable>\n\n";
	
}

sub funnel {

print $out "<!-- Main definitions - Output Hash MGs and Input Labels with Funnel name info -->\n";
print $out $ws1."<labelTable>\n";

$prepOutGroup = $dbh->prepare("select distinct outGroup from ".$db.".".$table." where outGroup is not null");
$prepOutGroup->execute();

$outGroup = $prepOutGroup->fetchall_arrayref();

foreach $outG (@$outGroup) {

	($outGr) = @$outG;

	print $out $ws2."<Output Group=\"$outGr\">\n";

	$prepInputLabel = $dbh->prepare("select distinct inputLabel from ".$db.".".$table." where outGroup=\"$outGr\"");
	$prepInputLabel->execute();

	$inputLabel = $prepInputLabel->fetchall_arrayref();

	foreach $input (@$inputLabel) {


		($inputL) = @$input;

		if ( defined $inputL ) {

		print $out $ws4."<InputLabel ID=\"$inputL\">\n";

		$prepFunnelName = $dbh->prepare("select distinct funnelName from ".$db.".".$table." where outGroup=\"$outGr\" and inputLabel=\"$inputL\"");
		$prepFunnelName->execute();

		$funnelName = $prepFunnelName->fetchall_arrayref();

		foreach $funnel (@$funnelName) {

			($funnelName) = @$funnel;
			
			print $out $ws5."<Funnel name=\"$funnelName\" Input=\"DDN\">\n";

			$prepFunnelName = $dbh->prepare("select instId,instName from ".$db.".".$table." where outGroup=\"$outGr\" and inputLabel=\"$inputL\" and funnelName=\"$funnelName\"");
			$prepFunnelName->execute();

			$inst = $prepFunnelName->fetchall_arrayref();

			foreach $ent (@$inst) {

				($inId,$inName) = @$ent;

				print $out $ws9."<inst ID=\"$inId\" system=\"$inName\" />\n";
				#print "out is $outGroup and input $inputL and funnel is $funnelName instId $inId instName $inName\n";
		
			}

			print $out $ws5."</Funnel>\n";
		}

		print $out $ws4."</InputLabel>\n";
		}
	}

	print $out $ws2."</Output>\n";

}

print $out $ws1."</labelTable>\n";
print $out "</FunnelLabels>\n";
	
}

sub labelX {

	$prepLab = $dbh->prepare("select distinct labelId from ".$db.".".$table." where labelId is not null order by labelId + 0;");
	$prepLab->execute();

	print $out $ws1."<!-- Labels to be excluded -->\n";
	print $out $ws1."<labels>\n";

	while (@lab = $prepLab->fetchrow_array()) {

		print $out $ws2."<label ID=\"$lab[0]\"/>\n";

	}

	print $out $ws1."</labels>\n";
	print $out "</ddn>\n";

}
