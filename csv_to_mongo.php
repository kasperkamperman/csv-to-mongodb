<?php

/*  https://www.kasperkamperman.com/blog/web/sync-csv-to-a-mongodb/
    kasperkamperman - 20-04-2021

    In the shell (mongo in terminal/command line)
    
    Make sure you create a collection with collation:

        use <your database>

        db.createCollection(
            "mycollection", 
            { collation: 
                { locale:"en",strength:1, alternate:"shifted", maxVariable:"punct"}
            }
        )
    
    Create an index:

        db.mycollection.createIndex(
            { "address": 1, "city": 1 }, 
            { unique: true } 
        )

*/ 

// connection string to local mongodb instance
$mongodb_url = 'mongodb://localhost:27017/?readPreference=primary&ssl=false';
//$csv_file = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQFaNt71w9-PyGF3rxH-ZtmyAUoeUVdgocK10rHG0IFtiK9fRoebmoLmAWEZ_X-H_fpWjJlPqEWbBb2/pub?output=csv';
$csv_file = 'library_locations.csv';

$dbase = 'mydatabase';
$collection = 'mycollection';

// if rows get removed from the sheet also remove them from the mongodb collection
$deleteMode = true;

//ini_set("default_charset", 'utf-8');
ini_set('display_errors', '1');
ini_set('error_reporting', E_ALL);

require_once __DIR__ . '/vendor/autoload.php';

/*  1. create a collection with collation
        locale en, nl is the same. 
        strength should be 1 (ignore diacritics and case)
        alternate should be "shifted"
            Whitespace and punctuation are not considered base characters.
        maxVariable should be "punct" (whitespace as punctuation)
    2. create a unique compound indexes, also configure collation

    In that way Enschede, enschede point are seen as the same index and thus
    will update instead of remove. 
*/

// used for aggregration pipeline
$client = new MongoDB\Client($mongodb_url);

// used for bulkwrite
$driver = new MongoDB\Driver\Manager($mongodb_url);
// https://www.php.net/manual/en/class.mongodb-driver-bulkwrite.php
// unordered so it continues and doesn't stop with duplicate keys or other errors
$mongo_bulk_write = new MongoDB\Driver\BulkWrite(['ordered' => false]);

// https://www.php.net/manual/en/function.str-getcsv.php
$csv = array_map('str_getcsv', file($csv_file));

$header = array_shift($csv); # remove first row as header

// used later to find deleted documents
$csv_compound;

foreach ($csv as $row) {

    // make it 
    $row = array_combine($header, $row);

    // skip if certain fields are empty.
    if(empty($row['address']) || empty($row['city'] || empty($row['name']) )) {
       continue;
    }

    // cleanup fields (used as compound unique index)
    $row['address'] = strtolower(preg_replace('/\s+/', ' ', $row['address']));
    $row['city']    = strtolower(preg_replace('/\s+/', ' ', $row['city']));
    $csv_compound[] = $row['city'].'#'.$row['address'];

    // remove empty columns from the row
    $unset = [];

    foreach ($row as $key => $val) {
        if(empty($val)) {
            unset($row[$key]);
            // MongoDB needs an associative array with empty values for unset
            $unset[$key] = 1;
        }
    }

    // update based on address and city. 
    $filter = [ 'city' => $row['city'], 'address' => $row['address'] ];

    if(empty($unset)) {
        $mongo_bulk_write->update($filter, [ '$set' => $row ], ['upsert' => true]);
    }
    else {
        $mongo_bulk_write->update($filter, [ '$set' => $row, '$unset' => $unset ], ['upsert' => true]);
    }

}

if($deleteMode) {

    /*  $group  : prepare an array with address and city fields combined. 
        $project: compare with setDifference with the csv_compound array made in PHP.
        
        The order in setDifference matters. The longest list (in this case mdb) should be left, so the differences 
        returned are the documents that are present in MongoDB, but not in the CSV anymore. 
        
        - https://stackoverflow.com/questions/31663037/given-a-list-of-ids-whats-the-best-way-to-query-which-ids-do-not-exist-in-the
        - https://stackoverflow.com/questions/24662413/select-records-matching-concat-value-of-two-fields-in-mongodb
    */

    $group    = ['$group' => [ '_id' => null, 'mdb_compound' => [ '$addToSet' => [ '$concat' => ['$city','#','$address'] ] ] ]];
    $project  = [ '$project' => [ 'difference_mdb-csv' => [ '$setDifference' => [ '$mdb_compound', $csv_compound] ], '_id' => 0 ] ];
    $pipeline = array($group, $project);

    // run the aggregation pipeline. 
    $cursor = $client->$dbase->$collection->aggregate($pipeline);

    // get an array with compound ('$city','#','$address') indexes
    $remove_from_mdb = json_decode(MongoDB\BSON\toJSON(MongoDB\BSON\fromPHP($cursor->toArray())), true);

    // delete the documents using address and city as compound index. 
    if($remove_from_mdb) {

        // get the right key
        $remove_from_mdb = $remove_from_mdb[0]['difference_mdb-csv'];

        foreach ($remove_from_mdb as $doc) {
            $doc = explode("#", $doc);
            $filter = [ 'city' => $doc[0], 'address' => $doc[1] ];

            // If the collation is unspecified but the collection has a default collation, 
            // the operation uses the collation specified for the collection.
            $mongo_bulk_write->delete($filter);
        }
    }
}

// bulkwrite to do insert, update and delete collections in MongoDb

try {
    $result = $driver->executeBulkWrite($dbase.'.'.$collection, $mongo_bulk_write);
}
catch (MongoDB\Driver\Exception\BulkWriteException $e) {
    
    $result = $e->getWriteResult();
    foreach ($result->getWriteErrors() as $error) {

        echo $error->getMessage()."\n";
        // parse the message to prepare data for later display
        // $data = explode('dup key: ', $error->getMessage(), 2)[1];
        // fix json data
        // $data = preg_replace('/(?<!")([a-zA-Z0-9_]+)(?!")(?=:)/i', '"$1"', $data);
        // $data = json_decode($data, true);
        // print_r($data);
    }

}

//https://www.php.net/manual/en/class.mongodb-driver-writeresult.php
printf("Upserted %d document(s)\n", $result->getUpsertedCount());
printf("Updated  %d document(s)\n", $result->getModifiedCount());
printf("Matched %d document(s)\n", $result->getMatchedCount());
printf("Deleted %d document(s)\n", $result->getDeletedCount());

echo "\n";

?>