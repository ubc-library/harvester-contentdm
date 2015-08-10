<?php
    /**
     * Created by PhpStorm.
     * User: hajime
     * Date: 09 Aug 15
     * Time: 17:22
     */
    
    namespace UBC\LSIT\Services\Elasticsearch;
    
    class ElasticsearchAgent {
        /**
         * @var $env string dev|verf|prod
         */
        public $env = "prod";
        public $api = "https://driad-back-dev.library.ubc.ca";

        /**
         * ElasticsearchAgent constructor.
         */
        public function __construct ($conf, $env) {

            $this->api = $this->getApiURI($env);



        }

        public function getApiURI ($env)
        {
            $prodProxy = "";

            $verfProxy = "";

            $devProxy = "/api"; //will resolve files against api.php

            /**
             * @var $proxy string API Ingest URL directory/proxy/front-file e.g. index.php
             */

            switch ($env) {
                case "core":
                    return "http://yahk.library.ubc.ca:9200";
                case "dev":
                case "verf":
                    $proxy = "{$env}Proxy";
                    return "htps://driad-back-{$env}.library.ubc.ca" . $$proxy;
                case "prod":
                    $proxy = "{$env}Proxy";
                    return "https://oc-index.library.ubc.ca" . $$proxy;
                default:
                    return "https://driad-back-dev.library.ubc.ca";
            }
        }

        /**
         * @param        $file
         * @param        $type
         * @param        $node
         * @param        $leaf
         * @param bool   $upsr
         * @param string $env dev|verf|prod
         *
         * @return int
         */
        public function flingToElasticsearch ($file, $type, $node, $leaf, $upsr = false, $env = 'verf')
        {
            //path to json file
            $path = "{$file}.{$type}";

            /**
             * @var $api string API Ingest URL
             */
            $api = self::getApiURI ($env);

            /**
             * @var $date_source string the repo the items are coming from - each warrants its own api processor section
             *
             * Can be one of [cdm,dsp,atm]
             */
            $data_source = "cdm";

            /**
             * @var $endpoint string resource/process requested
             */
            $endpoint = "/collections/{$node}/items/{$leaf}";

            /**
             * @var $api_key string access key
             */
            $api_key = "52c9fda9b2ebb7af6821a3900501fb4e0d694dcf54333db0fc5a9a7b4ae59d7c";

            $mode = $upsr ? "update" : "insert";

            $output_path = "-o/dev/null";// may want to log this

            # should not need to do much below
            $url = "{$api}{$endpoint}'?api_key='{$api_key}'&data_source='{$data_source}'&data_mode='{$mode}'&data_type='{$type}"; // remember to quote as needed to prevent command breaking when
            // passed to exec
            //echo "{$url}\n";
            $cmd = "curl -s -w '%{http_code}' {$output_path} -XPOST -H 'Content-type: multipart/form-data' --form payload=@{$path} {$url}";
            echo "Command: {$cmd}\n";
            //sleep(10);
            //return true;

            exec ($cmd, $res);
            echo "POSTing to ElasticSearch. Returned: " . $res[0] . "\n";
            if ($res[0] === 201) {
                $cmd = "rm {$path} > /dev/null 2>&1 &";
                exec ($cmd, $res);
            }

            return $res[0];

        }


        public function deleteRemoteIndex ($nick, $env = "verf")
        {

            /**
             * @var $api string API Ingest URL
             */
            $api = self::getApiURI ($env);

            /**
             * @var $endpoint string resource/process requested
             */
            $endpoint = strtolower ("/collections/{$nick}");


            /**
             * @var $trigger string the url to trigger
             */
            $trigger = "{$api}{$endpoint}";

            /**
             * @var $api_key string access key
             */
            $api_key = "52c9fda9b2ebb7af6821a3900501fb4e0d694dcf54333db0fc5a9a7b4ae59d7c";

            /**
             * @var $output_path string where the curl output will go
             */
            $output_path = "-o/dev/null";// may want to log this

            # should not need to do much below
            $url = "{$trigger}?api_key={$api_key}";
            echo "{$url}\n";
            $cmd = "curl -s -w '%{http_code}' {$output_path} -XDELETE {$url}";
            exec ($cmd, $res);
            $verb = ($res[0] == 410 || $res[0] == 404) ? "Yes" : "No";
            echo "   Remote Index [{$nick}] Deleted: {$verb} ({$res[0]})\n";
            sleep (5);

            return $res[0];
        }

        public function remoteIndexExists($nick, $env = "dev"){
            /**
             * @var $api string API Ingest URL
             */
            $api = self::getApiURI ($env);

            /**
             * @var $endpoint string resource/process requested
             */
            $endpoint = strtolower ("/collections/{$nick}");


            /**
             * @var $trigger string the url to trigger
             */
            $trigger = "{$api}{$endpoint}";

            /**
             * @var $api_key string access key
             */
            $api_key = "52c9fda9b2ebb7af6821a3900501fb4e0d694dcf54333db0fc5a9a7b4ae59d7c";

            /**
             * @var $output_path string where the curl output will go
             */
            $output_path = "-o /dev/null";// may want to log this

            # should not need to do much below
            $url = "{$trigger}?api_key={$api_key}";
            $cmd = "curl -s -w '%{http_code}' {$url} {$output_path}";
            exec ($cmd, $res);
            switch($res[0]){
                case 200:
                    return true;
                case 404:
                    return false;
                default:
                    echo "Unexpected Response: {$res[0]}\n";
                    return false;
            }
        }

        public function triggerRefresh ($index, $env = "core")
        {
            $action = "_refresh";
            $api = self::getApiURI ($env);
            $trigger = "{$api}/{$index}/{$action}";
            $cmd = "curl -s -XGET {$trigger}";

            return exec ($cmd, $res);
        }


        public function drillDownRemap ($haystackLabel, $haystackNick, $general, $specific = [], &$remap, $isSearchable)
        {
            # remap
            $key_general_lookup = [
                'publicat'       => 'publication'
                , 'publish'      => 'dc.publisher'
                , 'ispart'       => 'dc.isPartOf'
                , 'partof'       => 'dc.isPartOf'
                , 'categor'      => 'category'
                , 'identif'      => 'dc.identifier'
                , 'contentdm'    => '__contentdm'
                , 'repos'        => 'dc.source'
                , 'uuid'         => 'ubc.identifier.uuid'
                , 'licens'       => 'dcterms.license'
                , 'extent'       => 'dc.format.extent'
                , 'personalname' => 'dc.subject.hasPersonalNames'
                , 'website'      => 'dc.isReferencedBy'
                , 'project'      => 'dc.isReferencedBy'
                , 'note'         => "dc.description.note"
                , 'catalog'      => 'dc.isReferencedBy'
                , 'frequen'      => '_.custom.periodical_frequency'
                , 'place'        => 'dcterms.spatial'
                , "title"        => "dc.title"
                , "sort"         => "ubc.date.sort"
                , "format"       => "dc.format"
                , "genre"        => "dc.genre"
                , "contributor"  => "dc.contributor"
                , "creator"      => "dc.creator"
                , "description"  => "dc.description"
                , "langua"       => "dc.language"
                , "transc"       => "ubc.transcript"
                , "forms"        => "dc.isPartOf"
                , "callnumber"   => "ubc.identifier.callnumber"
                , "accession"    => "ubc.identifier.accession"
                , "date"         => "dc.date"
                , "rights"       => "dc.rights"
                , "source"       => "dc.source"
                , "type"         => "dc.type"
                , "transcript"   => "ubc.transcript"
                , "subject"      => "dc.subject"
                , "oclc"         => "_.custom.oclc"
                , "ubc"          => "dc.isReferencedBy"
            ];

            $key_specific_lookup = [
                'japane'       => 'japanese'
                , 'alterna'    => 'alternative'
                , 'alternate'  => 'alternative'
                , 'geographic' => 'geographic'
            ];

            $haystackString = str_replace (" ", "", $haystackLabel);
            $haystackString = strtolower ($haystackString);
            if (stripos ($haystackString, $general) !== false) {
                # if we don't find a more specific attribute to append, we return the general key
                $key = isset($key_general_lookup[$general]) ? $key_general_lookup[$general] : "{$general}";
                foreach ($specific as $map) {
                    if (stripos ($haystackString, $map) !== false) {
                        $key .= isset($key_specific_lookup[$map]) ? ".{$key_specific_lookup[$map]}" : ".{$map}";
                        $remap[$key][] = [
                            'label'  => $haystackLabel,
                            'key'    => $haystackNick,
                            'value'  => [],
                            'search' => $isSearchable
                        ];

                        return true;
                    }
                }
                $remap[$key][] = [
                    'label'  => $haystackLabel,
                    'key'    => $haystackNick,
                    'value'  => [],
                    'search' => $isSearchable
                ];

                return true;
            }

            return false;
        }

        static function getSortDateFromString ($dateAsString)
        {
            echo "\n   - Creating Sort Date:\n";
            if (is_array ($dateAsString)) {
                $dateAsString = implode ("", $dateAsString);//todo this is a crap fix and don't work on it, function expects a string!
            }
            //year era
            preg_match ("/(\d+)(?:.+bc){1}/i", $dateAsString, $bc_array);//catches bc and bce
            if (isset($bc_array) && isset($bc_array[1])) {
                return ["{$bc_array[1]} BC"];
            }
            //year era
            preg_match ("/(\d+)(?:.+century){1}/i", $dateAsString, $c_array);//catches bc and bce
            if (isset($c_array) && isset($c_array[1])) {
                $year = ($c_array[1] - 1) * 100;

                return ["{$year} AD"];
            }
            //year-month-day era
            //echo "YYYY-MM?-DD? AD\n";
            echo "     - date as string: " . (json_encode ($dateAsString)) . "\n";
            $timeParts = [];
            preg_match ('/([0-9\-\s]+)/i', $dateAsString, $timeParts);
            echo "     - time as parts: " . (json_encode ($timeParts)) . "\n";
            $date = preg_replace ("/([^\d]+)/", "-", trim ($timeParts[1]));
            $dateParts = explode ("-", $date);
            $y = trim ($dateParts[0]);
            $m = isset($dateParts[1]) && trim ($dateParts[1]) != "" ? trim ($dateParts[1]) : 12;
            $d = isset($dateParts[2]) && (trim ($dateParts[2]) != "") ? trim ($dateParts[2]) : ($m == 2 ? 28 : $m == 9 || $m == 4 || $m == 6 || $m == 11 ? 30 : 31);

            echo "   - sort date: {$y}-{$m}-{$d} AD\n";

            return "{$y}-{$m}-{$d} AD";
        }

        // this is in DRIADAgent because it maps fields specifically to how we want to refer to them in DRIAD
        public function getMappedFields ($fields, $numFields, $params)
        {
            $collectionName = $params['name'];
            $collectionPath = $params['path'];
            echo "  COLL NAME | {$collectionName}\n  DATA PATH | {$collectionPath}\n";

            # will be the list of fields, with their labels and corresponding nicks
            $remap = [];

            # OYE
            # unset all single entity fields (e.g. nick === title) and then remap the rest
            $cdmUniqueNicks = ['title', 'sort', 'date', 'rights', 'genre', 'reposi', 'forms', 'fullrs', 'langua', 'descri', 'dmcreated', 'dmmodified', 'dmrecord', 'find'];
            $key_unique_lookup = [
                "langua"       => "dc.language"
                , "title"      => "dc.title"
                , "sort"       => "ubc.date.sort"
                , "genre"      => "dc.genre"
                , "transc"     => "ubc.transcript"
                , "forms"      => "dc.isPartOf"
                , "date"       => "dc.date"
                , "rights"     => "dc.rights"
                , "type"       => "dc.type"
                , "reposi"     => "dc.source"
                , "transcript" => "ubc.transcript"
            ];

            # for each field, unset attributes not needed
            $attributesToDrop = ['req', 'admin', 'search', 'readonly', 'vocab', 'vocdb', 'dc'];

            echo " ----------------------------------------------------------------------------------------\n";
            printf (" %10s | %10s | %-60s\n", "FIELD", "TYPE", "NAME");
            echo " ----------------------------------------------------------------------------------------\n";

            # step 0.5 - get the fts field
            foreach ($fields as $unsetIndex => &$field) {
                # get rid of cdm specific attributes that we don't refer to
                foreach ($attributesToDrop as $attr) {
                    unset($field[$attr]);
                }
                if ($field['type'] == "FTS") {
                    printf (" %10s | %10s | %-60s\n", $field['nick'], $field['type'], $field['name']); //printf(" %10s | %s\n", $field['nick'], json_encode($field));
                    //$html .= "<tr><td>{$field['nick']}</td><td>{$field['type']}</td><td>{$field['name']}</td></tr>";
                    $key = isset($key_unique_lookup[$field['nick']]) ? $key_unique_lookup[$field['nick']] : $field['nick'];
                    $remap['ubc.transcript'][] = [
                        'label'  => $field['name'],
                        'key'    => $field['nick'],
                        'value'  => [],
                        'search' => true
                    ];
                    unset($fields[$unsetIndex]);
                }
                //safely ignore fields (but contentdm fields are also hidden, so need to not unset those)
                if ($field['hide'] == 1 && $field['nick'] != "find" && stripos ($field['nick'], "dm") !== 0) {
                    unset($fields[$unsetIndex]);
                }
            }

            # step 1 - get single entry fields
            foreach ($fields as $unsetIndex => &$field) {
                printf (" %10s | %10s | %-60s\n", $field['nick'], $field['type'], $field['name']); //printf(" %10s | %s\n", $field['nick'], json_encode($field));
                //$html .= "<tr><td>{$field['nick']}</td><td>{$field['type']}</td><td>{$field['name']}</td></tr>";
                if (in_array ($field['nick'], $cdmUniqueNicks)) {
                    $key = isset($key_unique_lookup[$field['nick']]) ? $key_unique_lookup[$field['nick']] : $field['nick'];
                    $remap[$key][] = [
                        'label'  => $field['name'],
                        'key'    => $field['nick'],
                        'value'  => [],
                        'search' => true
                    ];
                    unset($fields[$unsetIndex]);
                }
            }
            echo " ========================================================================================\n";
            echo " Field Count: {$numFields}\n";
            echo " ========================================================================================\n\n";

            # step 1.5 fake some shit for required fields that aren't set
            if ( !isset($remap['ubc.date.sort']) && isset($remap['dc.date'])) {
                $c = 0;
                $m = count ($remap['dc.date']);
                do {
                    if ($remap['dc.date'][$c]['key'] == "date") {
                        $f = $remap['dc.date'][$c];
                        $c = $m;
                        continue;
                    }
                    $f = $remap['dc.date'][$c];
                    $c++;
                } while ($c < $m);
                $remap['ubc.date.sort'][0] = $f;
                // do not move this ubc.date.sort -> it affects the date to format it regardless of the above
                $remap['ubc.date.sort'][0]['value'] = [];
                $remap['ubc.date.sort'][0]['key'] = 'sort';
            }

            $remap['ubc.date.sort'][0]['search'] = true;
            # self::remapAndUnset($remap,'key_to_lookup','key_to_use');
            self::remapAndUnset ($remap, 'descri', 'dc.description');
            self::remapAndUnset ($remap, 'forms', 'isPartOf');

            # get rid of contentdm system fields
            $remap['__contentdm']['date_created'] = $remap['dmcreated'];
            unset($remap['dmcreated']);

            $remap['__contentdm']['date_modified'] = $remap['dmmodified'];
            unset($remap['dmmodified']);

            $remap['__contentdm']['record_number'] = $remap['dmrecord'];
            unset($remap['dmrecord']);

            $remap['__contentdm']['find'] = $remap['find'];
            unset($remap['find']);

            # PRESERVE THE ORDER OF FIELDS, THIS IS A SPECIFICITY CHECK
            # do not use plurals!
            $titleNeedles = [
                'title', 'format', 'extent', 'date', 'source', 'creator', 'publish', 'publicat', 'rights', 'location', 'note', 'contributor', 'description', 'subject', 'transcript', 'personalname', 'genre', 'type', 'callnumber', 'accession', 'ispart', 'partof', 'identif', 'ubc', 'contentdm', 'oclc', 'uuid', 'repos', 'edition', 'licens', 'project', 'website', 'latitude', 'longitude', 'categor', 'catalog', 'bcbib', 'frequen', 'place', 'abstract'
            ];

            # these get added on to the needle above, e.g. this expands "title" to "title alternative" or "title alternate", "subject" to "subject geographic", etc
            $titleExtraNeedles = [
                'title'   => ['alternative', 'alternate', 'alterna'],
                'subject' => ['geographic']
            ];

            # step 2 - remap the leftover madness
            foreach ($fields as $field) {
                $isRemapped = false;
                foreach ($titleNeedles as $titleNeedle) {
                    $titleExtraNeedle = isset($titleExtraNeedles[$titleNeedle]) ? $titleExtraNeedles[$titleNeedle] : [];
                    # remap test - find all fields with [$titleNeedle]
                    if ($remapped = $this->drillDownRemap ($field['name'], $field['nick'], $titleNeedle, $titleExtraNeedle, $remap, true)) {
                        $isRemapped = true;
                        break;//break out of foreach $titleNeedles, back into loop foreach $fields
                    }
                }

                if ( !$isRemapped) {
                    $remap["ext_{$field['nick']}"][] = [
                        'label'  => $field['name'],
                        'key'    => $field['nick'],
                        'value'  => [],
                        'search' => false //false to not have to manually map
                    ];
                }

            }
            ksort ($remap);
            $debug_all_remaps [] = $remap;
            echo "\n";

            return $remap;
        }

        public function remapAndUnset (&$array, $lookup, $remap)
        {
            if (isset($array[$lookup])) {
                $array[$remap] = $array[$lookup];
                unset($array[$lookup]);
            }
        }
    }