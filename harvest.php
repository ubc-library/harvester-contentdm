#!/usr/bin/php
<?php
    /**
     * Created by PhpStorm.
     * User: skhanker
     * Date: 01/05/2015
     * Time: 3:39 PM
     */
    /*
    OYE! THIS IS LOCATED IN git:driad-back under:
    project_root -> src -> UBC -> LIST -> OpenCollections -> Ingest -> harvest.php
    Ensure changes are copied there so that the file is kept in git!
     */

    namespace Deprecated;

    include ('db.inc.php');

    class DRIADCollectionItem implements JsonSerializable
    {
        public $itemArray = [];

        public function __construct (array $array)
        {
            $this->itemArray = $array;
        }

        public function jsonSerialize ()
        {
            return $this->itemArray;
        }
    }

    $time_start = microtime (true);

    define('CTER', chr (27) . "[K\r"); //clear to end and carriage return
    define('CTEN', chr (27) . "[K\r\n"); //clear to end and newline
    define('SOURCE_ID', 1); // DRIAD source id of CONTENTdm
    define('REGEX_CDM_DELETED_dc.LOG', "/^([0-9]{4}\-[0-9]{2}\-[0-9]{2})\s((?:[0-9]{1,2}\:)*[0-9]{1,2})\s((?:[0-9]{1,3}\.){3}[0-9]{1,3})\s([a-z0-9]+)\s([a-z]+)\s([a-z0-9]+)$/mi");
    define('REGEX_CDM_SPLIT_CATALOGUE', "/(^(\/{1}[^\s]*)(?:\s*+)(\b.*?)(?:\s*+)(\/{1}[a-z0-9\/]+){1}(?:\s*)$)/mi");



    $conf = parse_ini_file ('config.ini');

    $ingester = new ContentDM();
    echo json_encode($ingester->getFieldMappings($conf['conf']));
    echo "\n";
    exit;

    $db = new DB($conf['dsn'], $conf['dbuser'], $conf['dbpass']);
    if ( !$db) {
        die("DB connection failed\n");
    }

    function displayHelpMessage ()
    {
        echo "____________________________________________________________________________________________________________________\n";
        echo "                                              INGEST HELP\n";
        echo "\n";
        echo "  --force-update       force an update of the collection\n";
        echo "  --drop-index         drop the specified collection(s) before adding them\n";
        echo "  --make-thumb         make thumbnails of each item (drills into compound object, first pages are always made)\n";
        echo "  --make-thumb-delay   make thumbnails of each item, after all ingesting takes place\n";
        echo "  --add-item           only add the specified item\n";
        echo "  --list-all           allow scanning over *all* collections (WARNING - THIS SHOULD BE RESERVED FOR CRON/SYSTEM)\n";
        echo "  --list-missing       lists all found nicks that aren't in ElasticSearch\n";
        echo "  --env                ingesting environment: dev|verf|prod|core\n";
        echo "  --cron-delete-multi  will trigger a scan of all the delete logs\n";
        echo "  --cron-delete        scans an individual delete log\n";
        echo "  --template           processing template to use\n";
        echo "  --debug-local        will not invoke XXXAgents (e.g. DRIADAgent, IIIFAgent) - prevents sending data to servers\n";
        exit;
    }

    $verbose = true;

    $colls = [];

    $flags = [
        '--force-update'      => false, // force item update
        '--drop-index'        => false, // drop index before adding
        '--make-thumb'        => false, // drop index before adding
        '--make-thumb-delay'  => false, // drop index before adding
        '--add-item'          => false, // specific item to add
        '--list-all'          => false, // specific item to add
        '--list-missing'      => false, // specific item to add
        '--all'               => false, // specific item to add
        '--cron-delete-multi' => false, // trigger a multi-delete call and exit
        '--cron-delete'       => false, // trigger a delete call and exit
        '--template'          => false, // trigger a delete call and exit
        '--debug-local'       => false  // prevent script from pushing to other servers on the network
    ];

    $index = null;
    $path = null;

    $environment = 'verf';

    array_shift ($argv);
    while ($arg = array_shift ($argv)) {
        //if it starts with a double dash then it's a flag, or else add it to the list of collections
        echo "Argument: [{$arg}]\n";
        switch ($arg) {
            case '--help':
                displayHelpMessage ();
                exit;
            case '--env':
                $environment = array_shift ($argv);
                break;
            case '--add-item':
            case '--template':
                $flags[$arg] = array_shift ($argv);
                break;
            case '--list-all':
            case '--list-missing':
            case '--drop-index':
            case '--make-thumb':
            case '--make-thumb-delay':
            case '--force-update':
            case '--all':
            case '--cron-delete-multi':
            case '--debug-local':
                $flags[$arg] = true;
                break;
            case '--cron-delete':
                $index = array_shift ($argv);
                $path = array_shift ($argv);
                if ( !isset($index) || $index == null) {
                    die("You must specify the index to delete");
                }
                if ( !isset($path) || $path == null) {
                    die("You must specify the path to search in");
                }
                $flags[$arg] = true;
                break;
            default:
                $colls [] = $arg;
        }
    }

    echo "\n\nPROCESSING PARAMS\n";
    echo "\nFLAGS:\n" . json_encode ($flags) . "\n\n";

    # exit if no collections set and no --all flag
    if ($colls === [] && !($flags['--list-all'] || $flags['--list-missing'])) {
        echo "You must specify a collection to harvest, or use the flag --all. Use --help for more.\n";
        exit;
    }

    $ingester = new ContentDM();

    if ($flags['--cron-delete-multi']) {
        $ingester->cronDeleteMulti ();
        exit;
    }

    if ($flags['--cron-delete']) {
        if ( !isset($index) || $index == null) {
            die("You must specify the index to delete");
        }
        if ( !isset($path) || $path == null) {
            die("You must specify the path to search in");
        }
        $ingester->cronDelete ($index, $path);
        exit;
    }

    //we need all collections at this point (regardless of the exit for --list-all)
    $allCollections = $ingester->getCollections ();

    if ($flags['--list-all']) {
        foreach ($allCollections as $nick => &$params) {
            echo "{$nick}\n";
        }
        exit;
    }

    if ($flags['--list-missing']) {
        echo "Collections not yet ingested:\n";
        foreach ($allCollections as $nick => &$params) {
            if(!DRIADAgent::remoteIndexExists($nick)){
                echo "{$nick}\n";
            }
        }
        echo "done!\n";
        exit;
    }


    if ($flags['--all']) {
        # nothing has to be done here, we use the $allCollections array
    } else {
        # drop all indices from allCollections except those passed on the command line
        $allCollections = array_intersect_key ($allCollections, array_flip ($colls));
    }

    echo "\n\nCOLLECTIONS:\n" . json_encode ($allCollections) . "\n\n\n\n";

    $processedCollectionsCount = 0;

    $debug_all_remaps = [];

    $html = "";

    foreach ($allCollections as $index => $params) {
        $processedCollectionsCount++;
        echo " # {$processedCollectionsCount}: PROCESSING {$index}\n ========================================================================================\n";
        if ($flags['--drop-index']) {
            DRIADAgent::deleteRemoteIndex ($index);
        }
        echo "\n  Path: {$params['path']}\n";

        // get collection field list
        $fields = json_decode ($ingester->getCollectionFieldInfo ($index), true);
        $numFields = count ($fields);

        //get fields array, with keys remapped as required by DRIAD
        $remap = DRIADAgent::getMappedFields ($fields, count ($fields), $params);

        // get items
        $payload = $ingester->query ($index, 1); // equivalent to dmQueryTotalRecs

        $records = []; //these are the DMRecords (CISOPTRs, POINTERS, all the same thing)


        $iiif_recs = [];


        if ($flags['--add-item']) {
            $total = 1;
            $records [0] = ['pointer' => $flags['--add-item']];
            echo " Getting specific dmrecord: {$flags['--add-item']}";
        } else {
            $start = 0;
            $pager = 512; # can be up to 1024, but this return size seems to be a sweet spot for contentdm/furry wrt processing speed
            $total = json_decode ($payload, true)['pager']['total'];
            // get all records in this index
            echo " Getting all 'dmrecords': ";
            while ($start < $total) {
                $ingester->debugMessage ("..", $verbose);
                $payload = $ingester->query ($index, $pager, $start);
                $recs = json_decode ($payload, true)['records'];
                foreach ($recs as $rec) {
                    $records [] = $rec;
                }
                $start += $pager;
            }
            echo " done!\n";
            // end get all dmrecords
        }

        echo "\n\n ========================================================================================\n Number of records to process: {$total}\n\n";
        sleep (1);
        echo "Alert: need to stop? You have 5s to cancel... processing in: ";
        foreach (["5..", "4..", "3..", "2..", "1..", "starting!\n"] as $num) {
            sleep (1);
            echo "$num";
        }
        sleep (1);

        if ( !$flags['--debug-local']) {
            $ingester->slack ("ContentDM Harvester - Starting Ingest: {$index} | Total # Records: {$total}", true);
        }

        // drill through each record in collection
        for ($i = 0; $i < $total; $i++) {
            echo "\n #{$i} - \n\n";
            $alias = "ingest.{$index}";
            $di = $records[$i]['pointer'];

            $info = $ingester->getItemInfo ($index, $di);
            $pages = json_decode ($info, true);

            $json = [];

            //TODO - put all this code in a foreach language, and set the language key on each iteration
            //TODO - create language testing code (sometime in future, CONTENTdm doesn't split up info based on language key (yet?...))
            //TODO - something like:  foreach ($pages as $entryLanguageKey => $v){}
            $entryLanguageKey = '_defaultLanguage';

            $json[$entryLanguageKey]['_json'] = $pages;
            $json[$entryLanguageKey]['_item'] = $ingester->mapValuesToFields ($json[$entryLanguageKey]['_json'], $remap);

            //after drill down, set some ubc related variables

            //image data
            echo " Getting image info ({$index}, {$di})\n";
            $imgInfo = json_decode ($ingester->getImageInfo ($index, $di), true);
            $ingester->parseImageInfo ($json[$entryLanguageKey]['_item'], $imgInfo);

            //pointers and handles with dmrecord
            $json[$entryLanguageKey]['_item']['ubc.internal.repo.handle'] = $di;
            $json[$entryLanguageKey]['_item']['ubc.internal.repo'] = "cdm";
            $json[$entryLanguageKey]['_item']['ubc.internal.cdm.find'] = "{$pages['find']}";
            $json[$entryLanguageKey]['_item']['ubc.internal.cdm.hasPDF'] = "{$pages['cdmprintpdf']}";

            //swap dmrecord with digital identifier, if possible
            $possibleRealID = '';
            if (isset($json[$entryLanguageKey]['_item']['dc.identifier'])) {
                $possibleRealID = implode ("", $ingester->getValueFromArrayOfArrays ($json[$entryLanguageKey]['_item']['dc.identifier'], "digitaliden"));
            }
            if (isset($possibleRealID) && $possibleRealID != '' && $possibleRealID != []) {
                $di = preg_replace ('/[^a-z0-9]/i', '_', $possibleRealID);
            }

            if(in_array($index,['chung2','bcbib'])){
                $nickOverrides = [
                    'chung2' => 'chung',
                    'bcbib' => 'bcbooks'
                ];
                $hdl = "cdm." . $nickOverrides[$index] . ".{$di}";
            } else {
                $hdl = "cdm." . strtolower ("{$index}") . ".{$di}";
            }
            $json[$entryLanguageKey]['_item']['ubc.internal.handle'] = $hdl;
            $json[$entryLanguageKey]['_item']['ubc.internal.hasTranscript'] = false;
            $json[$entryLanguageKey]['_item']['ubc.internal.downloads'] = [];
            $json[$entryLanguageKey]['_item']['ubc.internal.provenance.nick'] = strtolower ($index);

            // iiif uses the repo.handle to generate and image
            $iiif_recs[$json[$entryLanguageKey]['_item']['ubc.internal.handle']] = true;

            if (isset($json[$entryLanguageKey]['_item']['ubc.transcript']) && count ($json[$entryLanguageKey]['_item']['ubc.transcript'][0]['value']) > 0) {
                $json[$entryLanguageKey]['_item']['ubc.internal.hasTranscript'] = true;
            }

            if ( !isset($json[$entryLanguageKey]['_item']['ubc.transcript'])) {
                $json[$entryLanguageKey]['_item']['ubc.transcript'] = [""];
            }

            $missingSortDate = $json[$entryLanguageKey]['_item']['ubc.date.sort'][0]['value'] == [] ? true : false;
            // FIX SORT DATES
            if ($flags['--template'] == 'newspaper') {
                error_log ("  -- using an ingest template! these should be minimised as much as possible");
                foreach ($json[$entryLanguageKey]['_item']['dc.date'] as $date_entry) {
                    if ($date_entry['key'] == 'date') {
                        $json[$entryLanguageKey]['_item']['ubc.date.sort'][0]['value'] = $date_entry['value'];
                        $missingSortDate = false;
                    }
                }
            }
            //label fix - first resort because it may have sort
            if ($missingSortDate) {
                error_log ("  -- no sort nic found, setting to sort date label value");
                foreach ($json[$entryLanguageKey]['_item']['dc.date'] as $date_entry) {
                    $dateHaystack = str_replace (" ", "", strtolower ($date_entry['label']));
                    //echo "\n\n\n\n\nDate Haystack is {$dateHaystack}\n\n\n";
                    if ($missingSortDate && stripos ($dateHaystack, 'sort') !== false) {
                        echo "fixing sort date to " . json_encode ($date_entry['value']);
                        $json[$entryLanguageKey]['_item']['ubc.date.sort'][0]['value'] = $date_entry['value'];
                        $missingSortDate = false;
                    }
                }
            }
            //die;

            //very last resort, tak any "date", in order of publication/published, issue(d), date
            if ($missingSortDate) {
                error_log ("  -- no sort nic found, setting to sort date label value");
                $takeKeyInThisOrder = ['publi','issu','date'];
                foreach($takeKeyInThisOrder as $preferentialDateNeedle){
                    if(!$missingSortDate){
                        break;
                    }
                    foreach ($json[$entryLanguageKey]['_item']['dc.date'] as $date_entry) {
                        $dateHaystack = str_replace (" ", "", strtolower ($date_entry['label']));
                        if ($missingSortDate && stripos ($dateHaystack, $preferentialDateNeedle) !== false) {
                            echo "fixing sort date to " . json_encode ($date_entry['value']);
                            $json[$entryLanguageKey]['_item']['ubc.date.sort'][0]['value'] = $date_entry['value'];
                            $missingSortDate = false;
                        }
                    }
                }
            }

            // last resort, this could have values like "17th century" or "circa 800" which will result in failure
            if ($missingSortDate) {
                error_log ("  -- no sort nic found, setting to date nic value");
                foreach ($json[$entryLanguageKey]['_item']['dc.date'] as $date_entry) {
                    if ($missingSortDate && $date_entry['key'] == 'date') {
                        $json[$entryLanguageKey]['_item']['ubc.date.sort'][0]['value'] = $date_entry['value'];
                        $missingSortDate = false;
                    }
                }
            }

            if ($missingSortDate) {
                echo "\n\n{$hdl} is missing sort date, waiting 60 seconds to allow user cancel\n";
                //sleep (1);
            }


            if (is_array ($json[$entryLanguageKey]['_item']['ubc.date.sort'][0]['value'])) {
                $json[$entryLanguageKey]['_item']['ubc.date.sort'][0]['value'] = [DRIADAgent::getSortDateFromString ($json[$entryLanguageKey]['_item']['ubc.date.sort'][0]['value'][0])];
            } else {
                $json[$entryLanguageKey]['_item']['ubc.date.sort'][0]['value'] = [DRIADAgent::getSortDateFromString ($json[$entryLanguageKey]['_item']['ubc.date.sort'][0]['value'])];
            }

            $json[$entryLanguageKey]['_item']['ubc.internal.child.records'] = [];

            if ( !$flags['--debug-local']) {
                // check to see if item exists in ubc collection but is deleted in contentdm collection
                echo " Determining if item exists in remote Elasticsearch Index\n";
                $needToDrop = $ingester->remoteItemExists ($index, $pages['dmrecord']);
                $payload = json_decode ($needToDrop, true);
                if (isset($payload['data']['data']['hits']['total']) && $payload['data']['data']['hits']['total'] > 0) {
                    $payload = $payload['data']['data']['hits']['hits'][0];
                    $idToDrop = $payload['_id'];
                    $ret = $ingester->remoteDeleteExistingItem ($index, $idToDrop);
                    echo "  ------ RESULT ---- \n" . $ret . "\n";
                } else {
                    echo "  CISOPTR[{$pages['dmrecord']}] not in the remote index.\n";
                }
            }

            // test for a compound object
            $re = "/^(.+)(\\.cpd)$/i";
            $str = $pages['find'];
            preg_match ($re, $str, $matches);

            //will determine if item is compound etc, and get values accordingly
            $process = true;
            if ($process) {
                if (isset($matches) && count ($matches) == 0) {
                    $isCompound = false;
                    echo "\n\n Not Compound Object, nothing more to do...\n";
                }
                if (isset($matches) && count ($matches) == 3) {
                    echo "\n\n Processing Compound Object:\n";
                    // information on each page of the compound object
                    $compoundObj = json_decode ($ingester->getCompoundObjectInfo ($index, $pages['dmrecord']), true);
                    $compoundObjType = $compoundObj['type'];

                    if (array_key_exists ('node', $compoundObj)) {//this was a single item, read as an array instead of array of arrays
                        $compoundObjPage = $compoundObj['node']['page'];
                    } else if (array_key_exists ('pageptr', $compoundObj['page'])) {//this was a single item, read as an array instead of array of arrays
                        $compoundObjPage [] = $compoundObj['page'];
                    } else {
                        $compoundObjPage = $compoundObj['page'];
                    }


                    // drill through each page in the individual record
                    foreach ($compoundObjPage as $idxCtr => $page) {

                        echo "\n\n\n\n" . json_encode ($page) . "\n\n\n\n";

                        $childSearch = [
                            "dc.title"                      => ""
                            , "dc.title.alternative"        => ""
                            , "dc.description"              => ""
                            , "dc.subject"                  => ""
                            , "dcterms.spatial"             => ""
                            , "dc.subject.hasPersonalNames" => ""
                            , "dc.language"                 => ""
                            , "ubc.transcript"              => ""
                        ];
                        $ingester->debugMessage ("Internal Object: {$page['pagetitle']}, ptr-{$page['pageptr']} \n");

                        $pageInfo = $ingester->getItemInfo ($index, $page['pageptr']);
                        //echo "\n\n{$pageInfo}\n\n";
                        $temp = json_decode ($pageInfo, true);
                        $temp = $ingester->mapValuesToFields ($temp, $remap, true);

                        echo " Getting image info ({$index}, {$page['pageptr']})\n";
                        $imgInfo = json_decode ($ingester->getImageInfo ($index, $page['pageptr']), true);
                        $ingester->parseImageInfo ($temp, $imgInfo);
                        $temp['ubc.internal.handle'] = $hdl . "." . sprintf ("%04d", $idxCtr);//DO NOT REMOVE, ROD USES THIS

                        //RODs ubc.internal.handle = []
                        $temp['ubc.internal.child.offset'] = sprintf ("%04d", $idxCtr);//DO NOT REMOVE, ROD USES THIS

                        $temp['ubc.internal.repo.handle'] = "{$page['pageptr']}";
                        $temp['ubc.internal.cdm.find'] = "{$page['pagefile']}";

                        // iiif uses the repo.handle to generate and image
                        // NOTE - iiif should not generate child pages on ingest, these not needed for search, only for item view, make em wait
                        // $iiif_recs [] = $temp['ubc.internal.handle'];

                        if (isset($temp['ubc.date.sort'][0]['value'])) {
                            $temp['ubc.date.sort'][0]['value'] = [DRIADAgent::getSortDateFromString ($temp['ubc.date.sort'][0]['value'][0])];
                        }

                        if (isset($temp['ubc.transcript']) && count ($temp['ubc.transcript'][0]['value']) > 0) {
                            $json[$entryLanguageKey]['_item']['ubc.internal.hasTranscript'] = true;
                        }
                        if ( !isset($temp['ubc.transcript'])) {
                            $temp['ubc.transcript'] = [""];
                        }
                        $json[$entryLanguageKey]['_item']['ubc.internal.child.records'][] = $temp;
                    }
                }
            }
            if (isset($json[$entryLanguageKey]['_search'])) {
                unset($json[$entryLanguageKey]['_search']);
            }

            $json[$entryLanguageKey] = $json[$entryLanguageKey]['_item'];

            /** You put a fling here to submit each item in collection */

            echo "\n Writing JSON Ouput...\n";
            $filename = __DIR__ . "/out/{$hdl}.json";

            echo "\n Filename: {$filename}\n";
            file_put_contents ($filename, json_encode (new DRIADCollectionItem($json))); //correct format
            //gzip file
            $cmd = "cat {$filename} | gzip -cf > {$filename}.gz";
            exec ($cmd, $res);
            //delete non-gzipped file
            $cmd = "rm {$filename} > /dev/null 2>&1 &";
            exec ($cmd, $res);

            if ( !$flags['--debug-local']) {
                DRIADAgent::flingToElasticsearch ($filename, 'gz', strtolower ($index), $di, $flags['--force-update'], $environment);
                $sql = 'INSERT INTO collection_export("collection","handle","handle_modified") VALUES(:collection,:handle,:handle_modified)';
                $db->execute (
                    $sql, [
                        'collection'      => "cdm." . strtolower ($index),
                        'handle'          => $di,
                        'handle_modified' => date ("Y-m-d H:i:s", strtotime ($json['_defaultLanguage']['__contentdm']['date_modified'][0]['value']))
                    ]
                );
                $cmd = "rm {$filename}.gz > /dev/null 2>&1 &";
                exec ($cmd, $res);
                if ($flags['--make-thumb']) {
                    echo "Make thumb never works here without a sleep(1) as elasticsearch always needs min 1s to actually reflect the document that was just added";
                    //IIIFAgent::generateThumbnail ($hdl);
                }
            } else {
                echo "Last Modified (original) : {$json['_defaultLanguage']['__contentdm']['date_modified'][0]['value']}\n";
                echo "Last Modified (strtotime): " . strtotime ($json['_defaultLanguage']['__contentdm']['date_modified'][0]['value']) . "\n";
                echo "Last Modified (datefrmat): " . date ("Y-m-d H:i:s", strtotime ($json['_defaultLanguage']['__contentdm']['date_modified'][0]['value'])) . "\n\n";
                echo "\n\n" . json_encode ($json) . "\n\n\n\n                           10s TO COPY THE ABOVE JSON\n\n\n\n\n\n";
                echo " Running in '--debug-local' mode - look in the /out folder for your data\n";
                sleep (60);
            }
            echo "done!\n";
        }

        sleep (2);
        echo "   Triggering refresh on remote ES Index [$index]";
        DRIADAgent::triggerRefresh (strtolower ($index));
        sleep (4);

        // technically should always generate images after ingest. Whilst important, they are not needed initially for function, just speed/user experience
        // This can go here instead of after the fling to elasticsearch because it has to be explicitly called anyways
        if ($flags['--make-thumb-delay']) {
            IIIFAgent::generateThumbnails ($iiif_recs);
            $iiif_recs = [];
        }

        $end = time ();
        $time_end = microtime (true);
        $interval = $time_end - $time_start;
        echo "---- END COLLECTION [{$index}] ---- $interval \n\n";
    }

    echo "done!\n";
    $end = time ();
    $time_end = microtime (true);
    $interval = $time_end - $time_start;
    echo "---- END INGEST ---- $interval \n\n";

    exit;

    class DRIADAgent
    {

    }
