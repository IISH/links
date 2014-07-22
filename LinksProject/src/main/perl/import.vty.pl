#!/user/bin/perl

use DBI;
use XML::Simple;

# Buffer off
$| = 1;

# Check number of ARGS
if(@ARGV != 4){
	print 	"Error:\nWrong #no of arguments\nArgments. Found:" . @ARGV . 
			"\nPlease use perl import.pl [XML File] [Database] [id_source] [DROP AND CREATE] without '-'";
	
	# Close program
	exit;
}

# MySQL Database Part
$host = $ARGV[1];
$db = "links_a2a";

# Ask user for user and pass
print "Please enter database username:";
$user = <STDIN>;

print "Please enter database password:";
$pass = <STDIN>;

# Remove new lines
chomp $user;
chomp $pass;

# Connect
$dbh = DBI->connect("DBI:mysql:$db:$host", $user, $pass)
or die "Can't Connect to Database: $dbh->errstr\n";

# Create object
$xml = new XML::Simple;

# Read File
open(XMLFILE , $ARGV[0]);

# Read First line
$record = <XMLFILE>;

# Clean first line
$record =~ s/^.+?(<A2A\s+)/$1/gsxi;

my $snippet .= $record;

# Set Counter
my $counter = 1;
my $step = 10000;
my $stepper = $step;

# Drop and create table
if( $ARGV[3] == 1 ){
	$dbh->do("DROP TABLE IF EXISTS links_a2a.a2a");
	$dbh->do("DROP TABLE IF EXISTS links_a2a.person");
	$dbh->do("DROP TABLE IF EXISTS links_a2a.person_profession");
	$dbh->do("DROP TABLE IF EXISTS links_a2a.event");
	$dbh->do("DROP TABLE IF EXISTS links_a2a.object");
	$dbh->do("DROP TABLE IF EXISTS links_a2a.relation");
	$dbh->do("DROP TABLE IF EXISTS links_a2a.source");
	$dbh->do("DROP TABLE IF EXISTS links_a2a.source_sourceavailablescans_scan");
	$dbh->do("DROP TABLE IF EXISTS links_a2a.remark");

	$dbh->do(create_a2a());
	$dbh->do(create_person());
	$dbh->do(create_person_profession());
	$dbh->do(create_event());
	$dbh->do(create_object());
	$dbh->do(create_relation());
	$dbh->do(create_source());
	$dbh->do(create_source_sourceavailablescans_scan());
	$dbh->do(create_remark());
}

while ($record = <XMLFILE>) {
	
	# Add line to snippet
	$snippet .= $record;
	
	#Einde
	if($record =~ m/<\/A2A/ ){
		
		# get id
		$dbh->do( "INSERT INTO links_a2a.a2a() VALUES ()" );
		$a2a_id = $dbh->{mysql_insertid};
		
		# Process Snippet
		my (
			$query_person,
			$query_person_profession,
			$query_event,
			$query_object,
			$query_relation,
			$query_source,
			$query_source_sourceavailablescans_scan,
			$query_person_personname_personnameremark,
			$query_person_residence_detailplaceremark,
			$query_person_origin_detailplaceremark,
			$query_person_birthplace_detailplaceremark,
			$query_person_personremark,
			$query_event_eventplace_detailplaceremark,
			$query_event_eventremark,
			$query_object_objectremark,
			$query_source_sourceremark
		) = xmlProcess( $snippet, $a2a_id );
		
		
		# Execute
		if ($query_person ne ""){
			$dbh->do( $query_person );
		}
		if ($query_person_profession ne ""){
			$dbh->do( $query_person_profession );
		}
		if ($query_event ne ""){
			$dbh->do( $query_event );
		}
		if ($query_object ne ""){
			$dbh->do( $query_object );
		}
		if ($query_relation ne ""){
			$dbh->do( $query_relation );
		}
		if ($query_source ne ""){
			$dbh->do( $query_source );
		}
		if ($query_source_sourceavailablescans_scan ne ""){
			$dbh->do( $query_source_sourceavailablescans_scan );
		}
		if ($query_person_personname_personnameremark ne ""){
			$dbh->do( $query_person_personname_personnameremark );
		}
		if ($query_person_residence_detailplaceremark ne ""){
			$dbh->do( $query_person_residence_detailplaceremark );
		}
		if ($query_person_origin_detailplaceremark ne ""){
			$dbh->do( $query_person_origin_detailplaceremark );
		}
		if ($query_person_birthplace_detailplaceremark ne ""){
			$dbh->do( $query_person_birthplace_detailplaceremark );
		}
		if ($query_person_personremark ne ""){
			$dbh->do( $query_person_personremark );
		}
		if ($query_event_eventplace_detailplaceremark ne ""){
			$dbh->do( $query_event_eventplace_detailplaceremark );
		}
		if ($query_event_eventremark ne ""){
			$dbh->do( $query_event_eventremark );
		}
		if ($query_object_objectremark ne ""){
			$dbh->do( $query_object_objectremark );
		}
		if ($query_source_sourceremark ne ""){
			$dbh->do( $query_source_sourceremark );
		}
		

		$snippet = "";

		# Inform user
		if( $counter == $stepper ){
			print "$counter\n";
			$stepper += $step;
		}
		
		$counter++;
	}
}

close(XMLFILE);

# End

# $_[0] = String with CORRECT XML snippet
sub xmlProcess{

	# Load XML snippet
	my $data = $xml->XMLin($_[0], ForceArray => 1);
        my (%hash, $uniqID);
	
	# Used vars
	my $query_person 								= query_person();
	my $query_person_profession 					= query_person_profession();
	my $query_event 								= query_event();
	my $query_object 								= query_object();
	my $query_relation 								= query_relation();
	my $query_source 								= query_source();
	my $query_source_sourceavailablescans_scan 		= query_source_sourceavailablescans_scan();
	my $query_person_personname_personnameremark 	= query_remark();
	my $query_person_residence_detailplaceremark 	= query_remark();
	my $query_person_origin_detailplaceremark 		= query_remark();
	my $query_person_birthplace_detailplaceremark 	= query_remark();
	my $query_person_personremark 					= query_remark();
	my $query_event_eventplace_detailplaceremark 	= query_remark();
	my $query_event_eventremark 					= query_remark();
	my $query_object_objectremark 					= query_remark();
	my $query_source_sourceremark	 				= query_remark();
	
	# <Person>
	foreach my $person (@{$data->{Person}}) {
		
		$query_person .= "(";
		
		# Person id
		my $oldpid = $person->{pid}; # $pid =~ s/\D+//g;
		my $pid = $hash{$oldpid};
		unless ($pid)
		{
		    $uniqID++;
		    $hash{$oldpid} = $uniqID;
		    $pid = $hash{$oldpid};
		}
		
		$query_person .= $_[1] . ", " ; # Id for a2a (id_a2a)
		$query_person .= "'$pid', " ;
		
		# <PersonName><*>
		$query_person .= xml_to_mysql( $person->{PersonName}->[0]->{PersonNameLiteral}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{PersonName}->[0]->{PersonNameTitle}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{PersonName}->[0]->{PersonNameTitleOfNobility}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{PersonName}->[0]->{PersonNameFirstName}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{PersonName}->[0]->{PersonNameNickName}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{PersonName}->[0]->{PersonNameAlias}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{PersonName}->[0]->{PersonNamePatronym}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{PersonName}->[0]->{PersonNamePrefixLastName}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{PersonName}->[0]->{PersonNameLastName}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{PersonName}->[0]->{PersonNameFamilyName}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{PersonName}->[0]->{PersonNameInitials}->[0], 1 );
		
		# <PersonName><PersonNameRemark> (LOOP)
		foreach my $person_PersonName_PersonNameRemark (@{$person->{PersonName}->[0]->{PersonNameRemark}}) {
			$query_person_personname_personnameremark .= "(";
			$query_person_personname_personnameremark .= "$_[1], " ; # a2a id
			$query_person_personname_personnameremark .= "'person_personname_personnameremark', " ;
			$query_person_personname_personnameremark .= "'$pid', ";
			$query_person_personname_personnameremark .= xml_to_mysql( $person_PersonName_PersonNameRemark->{Key}, 1 );	
			$query_person_personname_personnameremark .= xml_to_mysql( $person_PersonName_PersonNameRemark->{Value}->[0], 0 );	
			$query_person_personname_personnameremark .= "),";
		}
		
		# <Gender>
		$query_person .= xml_to_mysql( $person->{Gender}->[0], 1 );
		
		# <Residence>
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{Country}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{Province}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{State}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{County}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{Place}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{Municipality}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{PartMunicipality}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{Block}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{Quarter}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{Street}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{DescriptiveLocationIndicator}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{HouseName}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{HouseNumber}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{HouseNumberAddition}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{Longitude}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Residence}->[0]->{Latitude}->[0], 1 );
		
		# <Residence><DetailPlaceRemark> (LOOP)		
		foreach my $person_Residence_DetailPlaceRemark (@{$person->{Residence}->[0]->{DetailPlaceRemark}}) {
			$query_person_residence_detailplaceremark .= "(";
			$query_person_residence_detailplaceremark .= "$_[1], " ; # a2a id
			$query_person_residence_detailplaceremark .= "'person_residence_detailplaceremark', " ;
			$query_person_residence_detailplaceremark .= "'$pid',";
			$query_person_residence_detailplaceremark .= xml_to_mysql( $person_Residence_DetailPlaceRemark->{Key}, 1);	
			$query_person_residence_detailplaceremark .= xml_to_mysql( $person_Residence_DetailPlaceRemark->{Value}->[0], 0);	
			$query_person_residence_detailplaceremark .= "),";
		}
		
		# <Religion>
		$query_person .= xml_to_mysql( $person->{Religion}->[0]->{PersonReligionLiteral}->[0], 1);	
		
		# <Origin>
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{Country}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{Province}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{State}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{County}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{Place}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{Municipality}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{PartMunicipality}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{Block}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{Quarter}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{Street}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{DescriptiveLocationIndicator}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{HouseName}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{HouseNumber}->[0], 1 );	
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{HouseNumberAddition}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{Longitude}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Origin}->[0]->{Latitude}->[0], 1 );
		
		# <Origin><DetailPlaceRemark> (LOOP)
		foreach my $person_Origin_DetailPlaceRemark (@{$person->{Origin}->[0]->{DetailPlaceRemark}}) {
			$query_person_origin_detailplaceremark .= "(";
			$query_person_origin_detailplaceremark .= $_[1] . ", " ; # a2a id
			$query_person_origin_detailplaceremark .= "'person_origin_detailplaceremark', " ;
			$query_person_origin_detailplaceremark .= "'$pid',";
			$query_person_origin_detailplaceremark .= xml_to_mysql( $person_Origin_DetailPlaceRemark->{Key}, 1);
			$query_person_origin_detailplaceremark .= xml_to_mysql( $person_Origin_DetailPlaceRemark->{Value}->[0], 0);
			$query_person_origin_detailplaceremark .= "),";
		}

		# <Age>
		$query_person .= xml_to_mysql( $person->{Age}->[0]->{PersonAgeLiteral}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Age}->[0]->{PersonAgeYears}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Age}->[0]->{PersonAgeMonths}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Age}->[0]->{PersonAgeWeeks}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Age}->[0]->{PersonAgeDays}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Age}->[0]->{PersonAgeHours}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{Age}->[0]->{PersonAgeMinutes}->[0], 1 );

		# <BirthDate>
		$query_person .= xml_to_mysql( $person->{BirthDate}->[0]->{LiteralDate}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthDate}->[0]->{Year}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthDate}->[0]->{Month}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthDate}->[0]->{Day}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthDate}->[0]->{Hour}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthDate}->[0]->{Minute}->[0], 1 );	
		
		# <BirthPlace>
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{Country}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{Province}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{State}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{County}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{Place}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{Municipality}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{PartMunicipality}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{Block}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{Quarter}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{Street}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{DescriptiveLocationIndicator}->[0], 1 );	
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{HouseName}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{HouseNumber}->[0], 1 );	
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{HouseNumberAddition}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{Longitude}->[0], 1 );
		$query_person .= xml_to_mysql( $person->{BirthPlace}->[0]->{Latitude}->[0], 1 );
		
		# <BirthPlace><DetailPlaceRemark> (LOOP)
		foreach my $person_BirthPlace_DetailPlaceRemark (@{$person->{BirthPlace}->[0]->{DetailPlaceRemark}}) {
			$query_person_birthplace_detailplaceremark .= "(";
				$query_person_birthplace_detailplaceremark .= $_[1] . ", " ; # a2a id
				$query_person_birthplace_detailplaceremark .= "'person_birthplace_detailplaceremark', " ;
				$query_person_birthplace_detailplaceremark .= "'$pid', ";
				$query_person_birthplace_detailplaceremark .= xml_to_mysql( $person_BirthPlace_DetailPlaceRemark->{Key}, 1);
				$query_person_birthplace_detailplaceremark .= xml_to_mysql( $person_BirthPlace_DetailPlaceRemark->{Value}->[0], 0);
			$query_person_birthplace_detailplaceremark .= "),";
		}
		
		# <Profession> (LOOP)
		foreach my $person_Profession (@{$person->{Profession}}) {
			$query_person_profession .= "(";
				$query_person_profession .= $_[1] . ", " ; # a2a id
				$query_person_profession .= xml_to_mysql( $pid, 1) ;
				$query_person_profession .= xml_to_mysql( $person_Profession, 0) ;
			$query_person_profession .= "),";
		}
		
		# <MaritalStatus>
		$query_person .= xml_to_mysql( $person->{MaritalStatus}->[0], 0) ;
		
		# <PersonRemark>
		foreach my $person_PersonRemark (@{$person->{PersonRemark}}) {
			$query_person_personremark .= "(";
				$query_person_personremark .= "$_[1], " ; # a2a id
				$query_person_personremark .= "'person_personremark', " ;
				$query_person_personremark .= "'$pid', ";
				$query_person_personremark .= xml_to_mysql( $person_PersonRemark->{Key}, 1) ;
				$query_person_personremark .= xml_to_mysql( $person_PersonRemark->{Value}->[0], 0) ;
			$query_person_personremark .= "),";
		}
		
		# End Person
		$query_person .= "),";
	}
	
	# <Event>
	foreach my $event (@{$data->{Event}}) {
	
		my $eid = $event->{eid};
		
		$query_event .= "(";
		$query_event .= $_[1] . ", " ; # a2a id
		$query_event .= xml_to_mysql( $eid, 1);
		
		# <EventType>
		$query_event .= xml_to_mysql( $event->{EventType}->[0], 1) ;
		
		# <EventDate>
		$query_event .= xml_to_mysql( $event->{EventDate}->[0]->{LiteralDate}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventDate}->[0]->{Year}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventDate}->[0]->{Month}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventDate}->[0]->{Day}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventDate}->[0]->{Hour}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventDate}->[0]->{Minute}->[0], 1);
		
		# <EventPlace>
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{Country}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{Province}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{State}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{County}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{Place}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{Municipality}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{PartMunicipality}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{Block}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{Quarter}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{Street}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{DescriptiveLocationIndicator}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{HouseName}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{HouseNumber}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{HouseNumberAddition}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{Longitude}->[0], 1);
		$query_event .= xml_to_mysql( $event->{EventPlace}->[0]->{Latitude}->[0], 1);
		
		# <EventPlace><DetailPlaceRemark> (LOOP)
		foreach my $event_EventPlace_DetailPlaceRemark (@{$event->{EventPlace}->[0]->{DetailPlaceRemark}}) {
			$query_event_eventplace_detailplaceremark .= "(";
				$query_event_eventplace_detailplaceremark .= $_[1] . ", " ; # a2a id
				$query_event_eventplace_detailplaceremark .= "'event_eventplace_detailplaceremark', " ;
				$query_event_eventplace_detailplaceremark .= xml_to_mysql( $eid, 1) ;
				$query_event_eventplace_detailplaceremark .= xml_to_mysql( $event_EventPlace_DetailPlaceRemark->{Key}, 1) ;
				$query_event_eventplace_detailplaceremark .= xml_to_mysql( $event_EventPlace_DetailPlaceRemark->{Value}->[0], 0) ;
			$query_event_eventplace_detailplaceremark .= "),";
		}
		
		# <EventReligion>
		$query_event .= xml_to_mysql( $event->{EventReligion}->[0]->{ReligionLiteral}->[0], 0);
		
		# End Event
		$query_event .= "),";
		
		# <EventRemark>
		foreach my $event_EventRemark (@{$event->{EventRemark}}) {
			$query_event_eventremark .= "(";
				$query_event_eventremark .= $_[1] . ", " ; # a2a id
				$query_event_eventremark .= "'query_event_eventremark', " ;
				$query_event_eventremark .= xml_to_mysql( $eid, 1) ;
				$query_event_eventremark .= xml_to_mysql( $event_EventRemark->{Key}, 1) ;
				$query_event_eventremark .= xml_to_mysql( $event_EventRemark->{Value}->[0], 0) ;
			$query_event_eventremark .= "),";
		}
		
		
	}
	
	# <Object>
	foreach my $object (@{$data->{Object}}) {

		my $oid = $object->{oid};
		
		$query_object .= "(";
			$query_object .= $_[1] . ", " ;
			$query_object .= xml_to_mysql( $oid, 1);
			$query_object .= xml_to_mysql( $object->{Description}->[0], 0);
		$query_object .= "),"; 
		
		# <ObjectRemark>		
		foreach my $object_ObjectRemark (@{$object->{ObjectRemark}}) {
			$query_object_objectremark .= "(";
				$query_object_objectremark .= $_[1] . ", " ; # a2a id
				$query_object_objectremark .= "'object_objectremark', " ;
				$query_object_objectremark .= xml_to_mysql( $oid, 1);
				$query_object_objectremark .= xml_to_mysql( $object_ObjectRemark->{Key}, 1);
				$query_object_objectremark .= xml_to_mysql( $object_ObjectRemark->{Value}->[0], 0);
			$query_object_objectremark .= "),";
		}
	}
	
	# <Relation>
	foreach my $relation (@{$data->{RelationEP}}) {
		my $PersonKeyRef = $relation->{PersonKeyRef}->[0];
		my $PersonKeyID = $hash{$PersonKeyRef};

		$query_relation .= "(";
		$query_relation .= $_[1] . ", " ; # a2a id
		$query_relation .= xml_to_mysql( "RelationEP", 1);
		$query_relation .= xml_to_mysql( $PersonKeyID, 1);
		$query_relation .= xml_to_mysql( $relation->{EventKeyRef}->[0], 1);
		$query_relation .= xml_to_mysql( $relation->{RelationType}->[0], 1);
		$query_relation .= xml_to_mysql( $relation->{ExtendedRelationType}->[0], 0);
		$query_relation .= "),";
	}
	foreach my $relation (@{$data->{RelationPP}}) {
                my $PersonKeyRef = $relation->{PersonKeyRef}->[0];
                my $PersonKeyID = $hash{$PersonKeyRef};

		$query_relation .= "(";
		$query_relation .= $_[1] . ", " ; # a2a id
		$query_relation .= xml_to_mysql( "RelationPP", 1);
		$query_relation .= xml_to_mysql( $PersonKeyID, 1);
		$query_relation .= xml_to_mysql( $relation->{PersonKeyRef}->[1], 1);
		$query_relation .= xml_to_mysql( $relation->{RelationType}->[0], 1);
		$query_relation .= xml_to_mysql( $relation->{ExtendedRelationType}->[0], 0);
		$query_relation .= "),";
	}
	foreach my $relation (@{$data->{RelationPO}}) {
                my $PersonKeyRef = $relation->{PersonKeyRef}->[0];
                my $PersonKeyID = $hash{$PersonKeyRef};

		$query_relation .= "(";
		$query_relation .= $_[1] . ", " ; # a2a id
		$query_relation .= xml_to_mysql( "RelationPO", 1);
		$query_relation .= xml_to_mysql( $PersonKeyID, 1);
		$query_relation .= xml_to_mysql( $relation->{ObjectKeyRef}->[0], 1);
		$query_relation .= xml_to_mysql( $relation->{RelationType}->[0], 1);
		$query_relation .= xml_to_mysql( $relation->{ExtendedRelationType}->[0], 0);
		$query_relation .= "),";
	}
	foreach my $relation (@{$data->{RelationEO}}) {
		$query_relation .= "(";
		$query_relation .= $_[1] . ", " ; # a2a id
		$query_relation .= xml_to_mysql( "RelationEO", 1);
		$query_relation .= xml_to_mysql( $relation->{EventKeyRef}->[0], 1);
		$query_relation .= xml_to_mysql( $relation->{ObjectKeyRef}->[0], 1);
		$query_relation .= xml_to_mysql( $relation->{RelationType}->[0], 1);
		$query_relation .= xml_to_mysql( $relation->{ExtendedRelationType}->[0], 0);
		$query_relation .= "),";
	}
	foreach my $relation (@{$data->{RelationP}}) {
                my $PersonKeyRef = $relation->{PersonKeyRef}->[0];
                my $PersonKeyID = $hash{$PersonKeyRef};

		$query_relation .= "(";
		$query_relation .= $_[1] . ", " ; # a2a id
		$query_relation .= xml_to_mysql( "RelationP", 1);
		$query_relation .= xml_to_mysql( $PersonKeyID, 1);
		$query_relation .= "NULL" . ", " ;
		$query_relation .= xml_to_mysql( $relation->{RelationType}->[0], 1);
		$query_relation .= xml_to_mysql( $relation->{ExtendedRelationType}->[0], 0);
		$query_relation .= "),";
	}
	foreach my $relation (@{$data->{RelationOO}}) {
		$query_relation .= "(";
		$query_relation .= $_[1] . ", " ; # a2a id
		$query_relation .= xml_to_mysql( "RelationOO", 1);
		$query_relation .= xml_to_mysql( $relation->{ObjectKeyRef}->[0], 1);
		$query_relation .= xml_to_mysql( $relation->{ObjectKeyRef}->[1], 1);
		$query_relation .= xml_to_mysql( $relation->{RelationType}->[0], 1);
		$query_relation .= xml_to_mysql( $relation->{ExtendedRelationType}->[0], 0);
		$query_relation .= "),";
	}
	foreach my $relation (@{$data->{RelationO}}) {
		$query_relation .= "(";
		$query_relation .= "$_[1], ";
		$query_relation .= xml_to_mysql( "RelationO", 1);
		$query_relation .= xml_to_mysql( $relation->{ObjectKeyRef}->[0], 1);
		$query_relation .= "NULL" . ", " ;
		$query_relation .= xml_to_mysql( $relation->{RelationType}->[0], 1);
		$query_relation .= xml_to_mysql( $relation->{ExtendedRelationType}->[0], 0);
		$query_relation .= "),";
	}	
	
	
	# <Source> ounce, but weuse for each for overview
	my $query_source = query_source() ;
	
	foreach my $source(@{$data->{Source}}) {

		$query_source .= "(";
		
		$query_source .= "$_[1], ";
		
		# <SourcePlace>
	    $query_source .= xml_to_mysql( $source->{SourcePlace}->[0]->{Country}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourcePlace}->[0]->{Province}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourcePlace}->[0]->{State}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourcePlace}->[0]->{Place}->[0], 1);
		
		# <SourceIndexDate>
		$query_source .= xml_to_mysql( $source->{SourceIndexDate}->[0]->{From}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourceIndexDate}->[0]->{To}->[0], 1);
		
		# <SourceDate>
		$query_source .= xml_to_mysql( $source->{SourceDate}->[0]->{LiteralDate}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourceDate}->[0]->{Year}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourceDate}->[0]->{Month}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourceDate}->[0]->{Day}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourceDate}->[0]->{Hour}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourceDate}->[0]->{Minute}->[0], 1);
		
		# <SourceType>
		$query_source .= xml_to_mysql( $source->{SourceType}->[0], 1);

		# <EAD>
		$query_source .= xml_to_mysql( $source->{EAD}->[0]->{URL}->[0], 1);
		$query_source .= xml_to_mysql( $source->{EAD}->[0]->{Code}->[0], 1);
	  
		# <EAC>
		$query_source .= xml_to_mysql( $source->{EAC}->[0]->{URL}->[0], 1);
		$query_source .= xml_to_mysql( $source->{EAC}->[0]->{Code}->[0], 1);
	  
		# <SourceReference>
		$query_source .= xml_to_mysql( $source->{SourceReference}->[0]->{Place}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourceReference}->[0]->{InstitutionName}->[0], 1);
		$query_source .= $ARGV[2] . ", " ;
		$query_source .= xml_to_mysql( $source->{SourceReference}->[0]->{Collection}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourceReference}->[0]->{Section}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourceReference}->[0]->{Book}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourceReference}->[0]->{Folio}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourceReference}->[0]->{Rolodeck}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourceReference}->[0]->{Stack}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourceReference}->[0]->{RegistryNumber}->[0], 1);
		$query_source .= xml_to_mysql( $source->{SourceReference}->[0]->{DocumentNumber}->[0], 1);
	 
		# <SourceAvailableScans> (LOOP)
		foreach my $source_SourceAvailableScans_Scan (@{$source->{SourceAvailableScans}->[0]->{Scan}}) {
			$query_source_sourceavailablescans_scan .= "(";
				$query_source_sourceavailablescans_scan .= $_[1] . ", " ; # a2a id
				$query_source_sourceavailablescans_scan .= xml_to_mysql( $source_SourceAvailableScans_Scan->{OrderSequenceNumber}->[0], 1);
				$query_source_sourceavailablescans_scan .= xml_to_mysql( $source_SourceAvailableScans_Scan->{Uri}->[0], 1);	
				$query_source_sourceavailablescans_scan .= xml_to_mysql( $source_SourceAvailableScans_Scan->{UriViewer}->[0], 1);
				$query_source_sourceavailablescans_scan .= xml_to_mysql( $source_SourceAvailableScans_Scan->{UriPreview}->[0], 0);
			$query_source_sourceavailablescans_scan .= "),";
		}
			 
		# <SourceDigitalizationDate>
		$query_source .= xml_to_mysql( $source->{SourceDigitalizationDate}->[0], 1);

		# <SourceLastChangeDate>
		$query_source .= xml_to_mysql( $source->{SourceLastChangeDate}->[0], 1);

		# <SourceDigitalOriginal>
		$query_source .= xml_to_mysql( $source->{SourceDigitalOriginal}->[0], 1);

		# <RecordIdentifier>
		$query_source .= xml_to_mysql( $source->{RecordIdentifier}->[0], 1);

		# <RecordGUID>
		$query_source .= xml_to_mysql( $source->{RecordGUID}->[0], 0);
		
		# <SourceRemark>
		foreach my $source_sourceremark (@{$source->{SourceRemark}}) {
			$query_source_sourceremark .= "(";
				$query_source_sourceremark .= $_[1] . ", " ; # a2a id
				$query_source_sourceremark .= "'source_sourceremark', " ;
				$query_source_sourceremark .= "NULL, ";
				$query_source_sourceremark .= xml_to_mysql( $source_sourceremark->{Key}, 1);
				$query_source_sourceremark .= xml_to_mysql( $source_sourceremark->{Value}->[0], 0);	
			$query_source_sourceremark .= "),";
		}
		
		# End Source
		$query_source .= ")" ;
	}
	
	# Remove last comma
	$query_person 								=~ s/\,$//;
	$query_person_profession 					=~ s/\,$//;
	$query_event 								=~ s/\,$//;
	$query_object 								=~ s/\,$//;
	$query_relation	 							=~ s/\,$//;
	$query_source 								=~ s/\,$//;
	$query_source_sourceavailablescans_scan 	=~ s/\,$//;
	$query_person_personname_personnameremark 	=~ s/\,$//;
	$query_person_residence_detailplaceremark 	=~ s/\,$//;
	$query_person_origin_detailplaceremark 		=~ s/\,$//;
	$query_person_birthplace_detailplaceremark 	=~ s/\,$//;
	$query_person_personremark 					=~ s/\,$//;
	$query_event_eventplace_detailplaceremark 	=~ s/\,$//;
	$query_event_eventremark 					=~ s/\,$//;
	$query_object_objectremark 					=~ s/\,$//;
	$query_source_sourceremark 					=~ s/\,$//;
	
	#empty half strings	
	if( $query_person								!~/\)$/) { $query_person	 							= ""; }
	if( $query_person_profession					!~/\)$/) { $query_person_profession 					= ""; }
	if( $query_event								!~/\)$/) { $query_event 								= ""; }
	if( $query_object								!~/\)$/) { $query_object 								= ""; }
	if( $query_relation								!~/\)$/) { $query_relation	 							= ""; }
	if( $query_source								!~/\)$/) { $query_source 								= ""; }
	if( $query_source_sourceavailablescans_scan		!~/\)$/) { $query_source_sourceavailablescans_scan 		= ""; }
	if( $query_person_personname_personnameremark	!~/\)$/) { $query_person_personname_personnameremark 	= ""; }
	if( $query_person_residence_detailplaceremark	!~/\)$/) { $query_person_residence_detailplaceremark 	= ""; }
	if( $query_person_origin_detailplaceremark		!~/\)$/) { $query_person_origin_detailplaceremark 		= ""; }
	if( $query_person_birthplace_detailplaceremark	!~/\)$/) { $query_person_birthplace_detailplaceremark	= ""; }
	if( $query_person_personremark					!~/\)$/) { $query_person_personremark 					= ""; }
	if( $query_event_eventplace_detailplaceremark	!~/\)$/) { $query_event_eventplace_detailplaceremark	= ""; }
	if( $query_event_eventremark					!~/\)$/) { $query_event_eventremark 					= ""; }
	if( $query_object_objectremark					!~/\)$/) { $query_object_objectremark 					= ""; }
	if( $query_source_sourceremark					!~/\)$/) { $query_source_sourceremark 					= ""; }
	
		
	# Return queries
	return (
		$query_person,
		$query_person_profession,
		$query_event,
		$query_object,
		$query_relation,
		$query_source,
		$query_source_sourceavailablescans_scan,
		$query_person_personname_personnameremark,
		$query_person_residence_detailplaceremark,
		$query_person_origin_detailplaceremark,
		$query_person_birthplace_detailplaceremark,
		$query_person_personremark,
		$query_event_eventplace_detailplaceremark,
		$query_event_eventremark,
		$query_object_objectremark,
		$query_source_sourceremark
	);
}

# Sub routine to get node
	# $_[0] = xml value
	# $_[1] = if true, retun comma
sub xml_to_mysql{
	my $returnVal = $_[0] ? $dbh->quote($_[0]) : "NULL" ;
	
	if($_[0] =~ m/HASH\(0x/){
		$returnVal = "NULL";
	} 
	
	if($_[1]){
		$returnVal .= ", ";
	}
	
	return $returnVal;
}

# Query Subroutines
sub query_person{
	return 
		"INSERT DELAYED IGNORE INTO links_a2a.person(
		a2a_id,
		pid,
		personnameliteral,
		personnametitle,
		personnametitleofnobility,
		personnamefirstname,
		personnamenickname,
		personnamealias,
		personnamepatronym,
		personnameprefixlastname,
		personnamelastname,
		personnamefamilyname,
		personnameinitials,
		gender,
		residence_country,
		residence_province,
		residence_state,
		residence_county,
		residence_place,
		residence_municipality,
		residence_partmunicipality,
		residence_block,
		residence_quarter,
		residence_street,
		residence_descriptivelocationindicator,
		residence_housename,
		residence_housenumber,
		residence_housenumberaddition,
		residence_longitude,
		residence_latitude,
		personreligionliteral,
		origin_country,
		origin_province,
		origin_state,
		origin_county,
		originplace,
		origin_municipality,
		origin_partmunicipality,
		origin_block,
		origin_quarter,
		origin_street,
		origin_descriptivelocationindicator,
		origin_housename,
		origin_housenumber,
		origin_housenumberaddition,
		origin_longitude,
		origin_latitude,
		personageliteral,
		personageyears,
		personagemonths,
		personageweeks,
		personagedays,
		personagehours,
		personageminutes,
		literaldate,
		year,
		month,
		day,
		hour,
		minute,
		birthplace_country,
		birthplace_province,
		birthplace_state,
		birthplace_county,
		birthplace_place,
		birthplace_municipality,
		birthplace_partmunicipality,
		birthplace_block,
		birthplace_quarter,
		birthplace_street,
		birthplace_descriptivelocationindicator,
		birthplace_housename,
		birthplace_housenumber,
		birthplace_housenumberaddition,
		birthplace_longitude,
		birthplace_latitude,
		maritalstatus
		) VALUES "
	;
}

sub query_person_profession{
	return
		"INSERT DELAYED IGNORE INTO links_a2a.person_profession(
		a2a_id,
		pid,
		content
		) VALUES "
	;
}

sub query_event{
	return
		"INSERT DELAYED IGNORE INTO links_a2a.event(
		a2a_id,
		eid,
		eventtype,
		literaldate,
		year,
		month,
		day,
		hour,
		minute,
		country,
		province,
		state,
		county,
		place,
		municipality,
		partmunicipality,
		block,
		quarter,
		street,
		descriptivelocationindicator,
		housename,
		housenumber,
		housenumberaddition,
		longitude,
		latitude,
		religionliteral
		) VALUES "
	;
}

sub query_object{
	return
		"INSERT DELAYED IGNORE INTO links_a2a.object(
		a2a_id,
		oid,
		description 
		) VALUES " 
	;
}

sub query_relation{
	return 
		"INSERT DELAYED IGNORE INTO links_a2a.relation(
		a2a_id,
		relation,
		keyref_1,
		keyref_2,
		relationtype,
		extendedrelationtype
		) VALUES " 
	;
}

sub query_source{
	return 
		"INSERT DELAYED IGNORE INTO links_a2a.source(
		a2a_id,
		country, 
		province, 
		state, 
		sourceplace_place, 
		sourceindexdate_from, 
		sourceindexdate_to, 
		literaldate, 
		year, 
		month, 
		day,
		hour,
		minute,
		sourcetype, 
		ead_url, 
		ead_code, 
		eac_url, 
		eac_code,
		sourcereference_place, 
		institutionname,
		archive,
		collection,
		section,
		book,
		folio, 
		rolodeck, 
		stack,
		registrynumber, 
		documentnumber, 
		sourcedigitalizationdate, 
		sourcelastchangedate,
		sourcedigitaloriginal, 
		recordidentifier, 
		recordguid
		) VALUES " 
	;
}

sub query_source_sourceavailablescans_scan{
	return 
		"INSERT DELAYED IGNORE INTO links_a2a.source_sourceavailablescans_scan(
		a2a_id,
		OrderSequenceNumber,
		Uri,
		UriViewer,
		UriPreview
		) VALUES "
	;
}

sub query_remark {
	return
		"INSERT DELAYED IGNORE INTO links_a2a.remark( 
		a2a_id,
		type,
		parent_id,
		remark_key,
		value
		) VALUES "
	;
}

#
#
#
# Query Create
sub create_a2a{
	return
		"CREATE TABLE a2a (
  		a2a_id int(11) NOT NULL AUTO_INCREMENT,
  		PRIMARY KEY (a2a_id)
		)"
	;
}

sub create_person{
	return 
		"CREATE TABLE links_a2a.person(
		id INT UNSIGNED NOT NULL AUTO_INCREMENT ,
		a2a_id INT UNSIGNED NULL ,
		pid VARCHAR(100) NULL DEFAULT NULL ,
		personnameliteral VARCHAR(100) NULL DEFAULT NULL ,
		personnametitle VARCHAR(100) NULL DEFAULT NULL ,
		personnametitleofnobility VARCHAR(100) NULL DEFAULT NULL ,
		personnamefirstname VARCHAR(100) NULL DEFAULT NULL ,
		personnamenickname VARCHAR(100) NULL DEFAULT NULL ,
		personnamealias VARCHAR(100) NULL DEFAULT NULL ,
		personnamepatronym VARCHAR(100) NULL DEFAULT NULL ,
		personnameprefixlastname VARCHAR(100) NULL DEFAULT NULL ,
		personnamelastname VARCHAR(100) NULL DEFAULT NULL ,
		personnamefamilyname VARCHAR(100) NULL DEFAULT NULL ,
		personnameinitials VARCHAR(100) NULL DEFAULT NULL ,
		gender VARCHAR(100) NULL DEFAULT NULL ,
		residence_country VARCHAR(100) NULL DEFAULT NULL ,
		residence_province VARCHAR(100) NULL DEFAULT NULL ,
		residence_state VARCHAR(100) NULL DEFAULT NULL ,
		residence_county VARCHAR(100) NULL DEFAULT NULL ,
		residence_place VARCHAR(100) NULL DEFAULT NULL ,
		residence_municipality VARCHAR(100) NULL DEFAULT NULL ,
		residence_partmunicipality VARCHAR(100) NULL DEFAULT NULL ,
		residence_block VARCHAR(100) NULL DEFAULT NULL ,
		residence_quarter VARCHAR(100) NULL DEFAULT NULL ,
		residence_street VARCHAR(100) NULL DEFAULT NULL ,
		residence_descriptivelocationindicator VARCHAR(100) NULL DEFAULT NULL ,
		residence_housename VARCHAR(100) NULL DEFAULT NULL ,
		residence_housenumber VARCHAR(100) NULL DEFAULT NULL ,
		residence_housenumberaddition VARCHAR(100) NULL DEFAULT NULL ,
		residence_longitude VARCHAR(100) NULL DEFAULT NULL ,
		residence_latitude VARCHAR(100) NULL DEFAULT NULL ,
		personreligionliteral VARCHAR(100) NULL DEFAULT NULL ,
		origin_country VARCHAR(100) NULL DEFAULT NULL ,
		origin_province VARCHAR(100) NULL DEFAULT NULL ,
		origin_state VARCHAR(100) NULL DEFAULT NULL ,
		origin_county VARCHAR(100) NULL DEFAULT NULL ,
		originplace VARCHAR(100) NULL DEFAULT NULL ,
		origin_municipality VARCHAR(100) NULL DEFAULT NULL ,
		origin_partmunicipality VARCHAR(100) NULL DEFAULT NULL ,
		origin_block VARCHAR(100) NULL DEFAULT NULL ,
		origin_quarter VARCHAR(100) NULL DEFAULT NULL ,
		origin_street VARCHAR(100) NULL DEFAULT NULL ,
		origin_descriptivelocationindicator VARCHAR(100) NULL DEFAULT NULL ,
		origin_housename VARCHAR(100) NULL DEFAULT NULL ,
		origin_housenumber VARCHAR(100) NULL DEFAULT NULL ,
		origin_housenumberaddition VARCHAR(100) NULL DEFAULT NULL ,
		origin_longitude VARCHAR(100) NULL DEFAULT NULL ,
		origin_latitude VARCHAR(100) NULL DEFAULT NULL ,
		personageliteral VARCHAR(100) NULL DEFAULT NULL ,
		personageyears VARCHAR(100) NULL DEFAULT NULL ,
		personagemonths VARCHAR(100) NULL DEFAULT NULL ,
		personageweeks VARCHAR(100) NULL DEFAULT NULL ,
		personagedays VARCHAR(100) NULL DEFAULT NULL ,
		personagehours VARCHAR(100) NULL DEFAULT NULL ,
		personageminutes VARCHAR(100) NULL DEFAULT NULL ,
		literaldate VARCHAR(100) NULL DEFAULT NULL ,
		year VARCHAR(100) NULL DEFAULT NULL ,
		month VARCHAR(100) NULL DEFAULT NULL ,
		day VARCHAR(100) NULL DEFAULT NULL ,
		hour VARCHAR(100) NULL DEFAULT NULL ,
		minute VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_country VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_province VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_state VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_county VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_place VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_municipality VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_partmunicipality VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_block VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_quarter VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_street VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_descriptivelocationindicator VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_housename VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_housenumber VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_housenumberaddition VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_longitude VARCHAR(100) NULL DEFAULT NULL ,
		birthplace_latitude VARCHAR(100) NULL DEFAULT NULL ,
		maritalstatus VARCHAR(100) NULL DEFAULT NULL ,
		PRIMARY KEY (id) )"
	;
}

sub create_person_profession{
	return
		"CREATE TABLE  links_a2a.person_profession(
		id INT UNSIGNED NOT NULL AUTO_INCREMENT ,
		a2a_id INT UNSIGNED NULL ,
		pid VARCHAR(100) NULL DEFAULT NULL ,
		content VARCHAR(100) NULL DEFAULT NULL ,
		PRIMARY KEY (id) )"
	;
}

sub create_event{
	return
		"CREATE TABLE links_a2a.event(
		id INT UNSIGNED NOT NULL AUTO_INCREMENT ,
		a2a_id INT UNSIGNED NULL ,
		eid VARCHAR(100) NULL DEFAULT NULL ,
		eventtype VARCHAR(100) NULL DEFAULT NULL ,
		literaldate VARCHAR(100) NULL DEFAULT NULL ,
		year VARCHAR(100) NULL DEFAULT NULL ,
		month VARCHAR(100) NULL DEFAULT NULL ,
		day VARCHAR(100) NULL DEFAULT NULL ,
		hour VARCHAR(100) NULL DEFAULT NULL ,
		minute VARCHAR(100) NULL DEFAULT NULL ,
		country VARCHAR(100) NULL DEFAULT NULL ,
		province VARCHAR(100) NULL DEFAULT NULL ,
		state VARCHAR(100) NULL DEFAULT NULL ,
		county VARCHAR(100) NULL DEFAULT NULL ,
		place VARCHAR(100) NULL DEFAULT NULL ,
		municipality VARCHAR(100) NULL DEFAULT NULL ,
		partmunicipality VARCHAR(100) NULL DEFAULT NULL ,
		block VARCHAR(100) NULL DEFAULT NULL ,
		quarter VARCHAR(100) NULL DEFAULT NULL ,
		street VARCHAR(100) NULL DEFAULT NULL ,
		descriptivelocationindicator VARCHAR(100) NULL DEFAULT NULL ,
		housename VARCHAR(100) NULL DEFAULT NULL ,
		housenumber VARCHAR(100) NULL DEFAULT NULL ,
		housenumberaddition VARCHAR(100) NULL DEFAULT NULL ,
		longitude VARCHAR(100) NULL DEFAULT NULL ,
		latitude VARCHAR(100) NULL DEFAULT NULL ,
		religionliteral VARCHAR(100) NULL DEFAULT NULL ,
		PRIMARY KEY (id) )"
	;
}

sub create_object{
	return
		"CREATE TABLE links_a2a.object(
		id INT UNSIGNED NOT NULL AUTO_INCREMENT ,
		a2a_id INT UNSIGNED NULL ,
		oid VARCHAR(100) NULL DEFAULT NULL ,
		description VARCHAR(100) NULL DEFAULT NULL ,
		PRIMARY KEY (id) )" 
	;
}

sub create_relation{
	return 
		"CREATE TABLE links_a2a.relation(
		id INT UNSIGNED NOT NULL AUTO_INCREMENT ,
		a2a_id INT UNSIGNED NULL ,
		relation VARCHAR(100) NULL DEFAULT NULL ,
		keyref_1 VARCHAR(100) NULL DEFAULT NULL ,
		keyref_2 VARCHAR(100) NULL DEFAULT NULL ,
		relationtype VARCHAR(100) NULL DEFAULT NULL ,
		extendedrelationtype VARCHAR(100) NULL DEFAULT NULL ,
		PRIMARY KEY (id) )" 
	;
}

sub create_source{
	return 
		"CREATE TABLE links_a2a.source(
		id INT UNSIGNED NOT NULL AUTO_INCREMENT ,
		a2a_id INT UNSIGNED NULL ,
		country VARCHAR(100) NULL DEFAULT NULL ,
		province VARCHAR(100) NULL DEFAULT NULL ,
		state VARCHAR(100) NULL DEFAULT NULL ,
		sourceplace_place VARCHAR(100) NULL DEFAULT NULL ,
		sourceindexdate_from VARCHAR(100) NULL DEFAULT NULL ,
		sourceindexdate_to VARCHAR(100) NULL DEFAULT NULL ,
		literaldate VARCHAR(100) NULL DEFAULT NULL ,
		year VARCHAR(100) NULL DEFAULT NULL ,
		month VARCHAR(100) NULL DEFAULT NULL ,
		day VARCHAR(100) NULL DEFAULT NULL ,
		hour VARCHAR(100) NULL DEFAULT NULL ,
		minute VARCHAR(100) NULL DEFAULT NULL ,
		sourcetype VARCHAR(100) NULL DEFAULT NULL ,
		ead_url VARCHAR(100) NULL DEFAULT NULL ,
		ead_code VARCHAR(100) NULL DEFAULT NULL ,
		eac_url VARCHAR(100) NULL DEFAULT NULL ,
		eac_code VARCHAR(100) NULL DEFAULT NULL ,
		sourcereference_place VARCHAR(100) NULL DEFAULT NULL ,
		institutionname VARCHAR(100) NULL DEFAULT NULL ,
		archive VARCHAR(100) NULL DEFAULT NULL ,
		collection VARCHAR(100) NULL DEFAULT NULL ,
		section VARCHAR(100) NULL DEFAULT NULL ,
		book VARCHAR(100) NULL DEFAULT NULL ,
		folio VARCHAR(100) NULL DEFAULT NULL ,
		rolodeck VARCHAR(100) NULL DEFAULT NULL ,
		stack VARCHAR(100) NULL DEFAULT NULL ,
		registrynumber VARCHAR(100) NULL DEFAULT NULL ,
		documentnumber VARCHAR(100) NULL DEFAULT NULL ,
		sourcedigitalizationdate VARCHAR(100) NULL DEFAULT NULL ,
		sourcelastchangedate VARCHAR(100) NULL DEFAULT NULL ,
		sourcedigitaloriginal VARCHAR(100) NULL DEFAULT NULL ,
		recordidentifier VARCHAR(100) NULL DEFAULT NULL ,
		recordguid VARCHAR(100) NULL DEFAULT NULL ,
		PRIMARY KEY (id) )"
	;
}

sub create_source_sourceavailablescans_scan {
	return 
		"CREATE TABLE links_a2a.source_sourceavailablescans_scan(
		id INT UNSIGNED NOT NULL AUTO_INCREMENT ,
		a2a_id INT UNSIGNED NULL ,
		OrderSequenceNumber VARCHAR(100) NULL DEFAULT NULL ,
		Uri VARCHAR(100) NULL DEFAULT NULL ,
		UriViewer VARCHAR(100) NULL DEFAULT NULL ,
		UriPreview VARCHAR(100) NULL DEFAULT NULL ,
		PRIMARY KEY (id) )"
	;
}

sub create_remark {
	return
		"CREATE TABLE links_a2a.remark (
		id INT UNSIGNED NOT NULL AUTO_INCREMENT ,
		a2a_id INT UNSIGNED NULL ,
		type VARCHAR(100) NULL DEFAULT NULL ,
		parent_id VARCHAR(100) NULL DEFAULT NULL ,
		remark_key VARCHAR(100) NULL DEFAULT NULL ,
		value VARCHAR(100) NULL DEFAULT NULL ,
		PRIMARY KEY (id) )"
	;
}
