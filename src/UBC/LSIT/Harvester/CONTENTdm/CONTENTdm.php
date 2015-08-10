<?php
    /**
     * Created by PhpStorm.
     * User: hajime
     * Date: 09 Aug 15
     * Time: 14:40
     */
    
    namespace UBC\LSIT\Harvester\CONTENTdm;

    use DateTime;
    use DateTimeZone;
    
    class CONTENTdm {


        protected $CONTENTdmAPI = "http://cdmbackend.library.ubc.ca/dmwebservices/index.php?q=";

        protected $CONTENTdmATimeout = 300;

        protected $collectionManager;

        protected $dropFromCatalog;

        /**
         * CONTENTdm constructor.
         */
        public function __construct ($conf) {

            $this->dropFromCatalog = $conf['collection_exclude'];

        }

        public function collections () {
            return new Collections();
        }

        public function getCollectionList (
            $fmt = 'json'
        ) {
            $queryURL = "dmGetCollectionList/{$fmt}";

            return $this->doRequest ($this->CONTENTdmAPI . $queryURL);
        }

        public function getCollections ()
        {
            $CDMCatalog = __DIR__ . "/../catalog.txt";
            $allCollections = [];
            if (file_exists ($CDMCatalog)) {
                $handle = fopen ($CDMCatalog, "r");
                if ($handle) {
                    while (($logLine = fgets ($handle)) !== false) {
                        $re = "/\\/(.+?)\\t+(.+?)\\t+(\\/.+)/";
                        //preg_match ("/^(\/{1}[^\s]*)(?:\s*+)(\b.*?)(?:\s*+)(\/{1}[a-z0-9\/]+){1}(?:\s*)$/i", $logLine, $matches);
                        preg_match ($re, $logLine, $matches);
                        if (isset($matches) && count ($matches) > 0) {
                            $allCollections [trim(str_replace ("/", "", $matches[1]))] = [
                                'nick' => trim($matches[1]), //collection nick
                                'name' => $matches[2], //full collection title
                                'path' => $matches[3]  //path to collection data
                            ];
                        } else {
                            // error_log ("Log line {$logLine} did not match regexp");
                        }
                    }
                    fclose ($handle);
                } else {
                    error_log ("Could not open the file {$CDMCatalog}");
                }
            }
            ksort ($allCollections);
            $dropFromCatalog = $this->dropFromCatalog;
            foreach ($dropFromCatalog as $toDrop) {
                if (isset($allCollections[$toDrop])) {
                    unset($allCollections[$toDrop]);
                }
            }

            return $allCollections;
        }

        public function getCollectionParameters (
            $n,
            $fmt = 'json'
        ) {

            // use getCollections instead of this!
            $queryURL = "dmGetCollectionParameters/{$n}/{$fmt}";

            return $this->doRequest ($this->CONTENTdmAPI . $queryURL);

        }

        /**
         * @param string $n
         * @param string $fmt
         *
         * @return mixed
         */
        public function getCollectionFieldInfo (
            $n = 'all',
            $fmt = 'json'
        ) {
            $queryURL = "dmGetCollectionFieldInfo/{$n}/{$fmt}";
            $requestURL = $this->CONTENTdmAPI . $queryURL;

            return $this->doRequest ($requestURL);

        }

        private function doRequest ($url)
        {
            $ch = curl_init ();
            curl_setopt ($ch, CURLOPT_USERAGENT, "Mozilla/5.0 (Windows; U; Windows NT 5.1; rv:1.7.3) Gecko/20041001 Firefox/0.10.1");
            curl_setopt ($ch, CURLOPT_URL, $url);
            curl_setopt ($ch, CURLOPT_SSL_VERIFYPEER, false);    # required for https urls
            curl_setopt ($ch, CURLOPT_CONNECTTIMEOUT, $this->CONTENTdmATimeout);
            curl_setopt ($ch, CURLOPT_TIMEOUT, $this->CONTENTdmATimeout);
            curl_setopt ($ch, CURLOPT_RETURNTRANSFER, true); //curl_setopt ($ch, CURLOPT_POST,1); curl_setopt ($ch, CURLOPT_POSTFIELDS, $payload);
            $content = curl_exec ($ch);
            $response = curl_getinfo ($ch);
            curl_close ($ch);

            return $content;
        }

        public function remoteItemExists (
            $n,
            $i
        ) {
            $url = "https://driad-back-dev.library.ubc.ca/api/search";
            $payload = '{"index": "' . $n . '","type": "object","body": {"query": {"term": {"ubc.internal.cdm.handle": "' . $i . '"}}}}';
            //echo $payload . "\n\n";
            $ch = curl_init ();
            curl_setopt ($ch, CURLOPT_USERAGENT, "Mozilla/5.0 (Windows; U; Windows NT 5.1; rv:1.7.3) Gecko/20041001 Firefox/0.10.1");
            curl_setopt ($ch, CURLOPT_URL, $url);
            curl_setopt ($ch, CURLOPT_SSL_VERIFYPEER, false);    # required for https urls
            curl_setopt ($ch, CURLOPT_CONNECTTIMEOUT, $this->CONTENTdmATimeout);
            curl_setopt ($ch, CURLOPT_TIMEOUT, $this->CONTENTdmATimeout);
            curl_setopt ($ch, CURLOPT_RETURNTRANSFER, true);
            curl_setopt ($ch, CURLOPT_POST, 1);
            curl_setopt ($ch, CURLOPT_POSTFIELDS, $payload);
            $content = curl_exec ($ch);
            $response = curl_getinfo ($ch);
            curl_close ($ch);

            return $content;
        }

        public function remoteDeleteExistingItem (
            $n,
            $i
        ) {
            $url = "https://driad-back-dev.library.ubc.ca/api/collections/{$n}/items/{$i}";
            $ch = curl_init ();
            curl_setopt ($ch, CURLOPT_USERAGENT, "Mozilla/5.0 (Windows; U; Windows NT 5.1; rv:1.7.3) Gecko/20041001 Firefox/0.10.1");
            curl_setopt ($ch, CURLOPT_URL, $url);
            curl_setopt ($ch, CURLOPT_SSL_VERIFYPEER, false);    # required for https urls
            curl_setopt ($ch, CURLOPT_CONNECTTIMEOUT, $this->CONTENTdmATimeout);
            curl_setopt ($ch, CURLOPT_TIMEOUT, $this->CONTENTdmATimeout);
            curl_setopt ($ch, CURLOPT_CUSTOMREQUEST, "DELETE");
            $content = curl_exec ($ch);
            $response = curl_getinfo ($ch);
            curl_close ($ch);

            return $content;
        }

        public function remoteDeleteExistingItems ($index, $collectionPath)
        {
            $ingestLog = __DIR__ . "/out/cdm.{$index}.log";
            if (file_exists ($ingestLog)) {
                $ingest_date = date ("F d Y H:i:s.", filemtime ($ingestLog));
            } else {
                echo "Could not find ingest log, creating fake last ingest date.\n";
                $ingest_date = "1969-01-01 00:00:01";
            }

            $date = new DateTime($ingest_date, new DateTimeZone('America/Vancouver'));
            echo "Ingest on {$index} was last completed: " . $date->getTimestamp () . "\n";

            // find deleted files
            $deleteLog = "{$collectionPath}/index/description/delete.log";
            if (file_exists ($deleteLog)) {
                $handle = fopen ($deleteLog, "r");
                if ($handle) {
                    while (($logLine = fgets ($handle)) !== false) {
                        $matches = [];
                        //$logLineArr = [];
                        preg_match ("/([0-9]{4}\-[0-9]{2}\-[0-9]{2})\s((?:[0-9]{1,2}\:)*[0-9]{1,2})\s((?:[0-9]{1,3}\.){3}[0-9]{1,3})\s([a-z0-9]+)\s([a-z]+)\s([a-z0-9]+)/i", $logLine, $matches);
                        if (isset($matches) && count ($matches) > 0) {
                            $logLineArr = [
                                'date' => $matches[1], //date
                                'time' => $matches[2], //time
                                'ipdr' => $matches[3], //ip address
                                'user' => $matches[4], //user who initiated command
                                'comd' => $matches[5], //command
                                'pntr' => $matches[6]  //pointer of the record (also dmrecord?)
                            ];
                            $dateLog = new DateTime("{$logLineArr['date']} {$logLineArr['time']}", new DateTimeZone('America/Vancouver'));
                            if ($dateLog > $date) {
                                // if you get an item back, you need to drop it
                                $needToDrop = $this->remoteItemExists ($index, $logLineArr['pntr']);
                                $payload = json_decode ($needToDrop, true);
                                if (isset($payload['data']['data']['hits']['total']) && $payload['data']['data']['hits']['total'] > 0) {
                                    $payload = $payload['data']['data']['hits']['hits'][0];
                                    $idToDrop = $payload['_id'];
                                    $ret = $this->remoteDeleteExistingItem ($index, $idToDrop);
                                    echo "\n ------ RESULT ---- \n" . $ret . "\n";
                                } else {
                                    echo "CISOPTR[{$logLineArr['pntr']}] not in the remote index.\n";
                                }
                            } else {
                                echo "Skipped log entry, log date ({$logLineArr['date']} {$logLineArr['time']}) < ingest date ({$ingest_date})\n";
                            }
                        } else {
                            error_log ("Log line did not match regexp");
                        }
                    }
                    fclose ($handle);
                } else {
                    error_log ("Could not open the file {$deleteLog}");
                }
            }
        }

        public function getItemInfo (
            $n,
            $i,
            $fmt = 'json'
        ) {
            $queryURL = "dmGetItemInfo/{$n}/{$i}/{$fmt}";

            $requestURL = $this->CONTENTdmAPI . $queryURL;

            return $this->doRequest ($requestURL);
        }

        public function getImageInfo (
            $n,
            $i,
            $fmt = 'xml'
        ) {
            $queryURL = "dmGetImageInfo/{$n}/{$i}/{$fmt}";

            $requestURL = $this->CONTENTdmAPI . $queryURL;

            return $this->xmlToJson ($this->doRequest ($requestURL));
        }

        public function xmlToJson ($xml)
        {
            //$fileContents = file_get_contents ($url);
            //$fileContents = str_replace (["\n", "\r", "\t"], '', $xml);
            //$fileContents = trim (str_replace ('"', "'", $fileContents));
            $simpleXml = simplexml_load_string ($xml);
            $json = json_encode ($simpleXml);

            return $json;
        }

        public function getCompoundObjectInfo (
            $n,
            $i,
            $fmt = 'json'
        ) {
            $queryURL = "dmGetCompoundObjectInfo/{$n}/{$i}/{$fmt}";

            $requestURL = $this->CONTENTdmAPI . $queryURL;

            return $this->doRequest ($requestURL);
        }


        /**
         * @param string $n
         * @param string $q
         * @param string $fields
         * @param string $sortby
         * @param int    $maxrecs
         * @param int    $start
         * @param int    $suppress
         * @param int    $ptr
         * @param int    $suggest
         * @param int    $f
         * @param int    $showunpub
         * @param int    $cf
         * @param string $fmt
         *
         * @return string
         *
         *
         * To get all items, query $nic with maxrecs = 0 and start = 0 and iterate through pager->total
         *
         * returns [
         *  'pager'     => [
         *          'start' => n,
         *          'maxrecs' => n,
         *          'total' => n
         *  ],
         *  'records'   => [],
         *  'facets'    => []
         * ]
         */
        public function query (
            $n = 'all',
            $maxrecs = 250,
            $start = 0,
            $q = 'CISOSEARCHALL',
            $fields = '',
            $sortby = '',
            $suppress = 0,
            $ptr = 0,
            $suggest = 0,
            $f = 0,
            $showunpub = 1,
            $cf = 0,
            $fmt = 'json'
        ) {

            $queryURL = "dmQuery/{$n}/{$q}/{$fields}/{$sortby}/{$maxrecs}/{$start}/{$suppress}/{$ptr}/{$suggest}/{$f}/{$showunpub}/{$cf}/{$fmt}";

            $requestURL = $this->CONTENTdmAPI . $queryURL;

            return $this->doRequest ($requestURL);
        }

        public function getFieldMappings ($conf) {
            $mapping = [];

            $mapFiles = [
                "map_oai_dc.ini"
                , "map_oai_qdc.ini"
            ];

            foreach ($mapFiles as $map) {
                $mapping = array_merge ($mapping, parse_ini_file(__DIR__ . "/{$conf}/$map"));
            }

            return $mapping;
        }

        public function getLocale () { }

        public function getCollectionArchivalInfo () { }

        public function getCollectionPDFInfo () { }

        public function getCollectionDisplayImageSettings () { }

        public function getCollectionImageSettings () { }

        public function debugMessage ($message, $isVerbose = true)
        {
            if ($isVerbose) {
                echo $message;
            }
        }

        //CDM organizes the "supp" directory like this:
        //objects 1-9999: each has a directory under supp/
        //objects >= x*10^5: each has a directory under supp/Dx0000/
        function rodGetSuppDirectory ($path, $ptr)
        {
            $path = rtrim ($path, '/') . '/supp';
            if ($ptr >= 10000) {
                $path .= '/D' . floor ($ptr / 10000) . '0000';
            }
            $path .= '/' . $ptr;

            return $path;
        }

        public function slack ($message, $sendTeaser = false, $room = "random", $icon = ":ghost:")
        {
            return 1;
            $users = [
                "skhanker", "sean", "kevinho", "schuyberg", "rod"
            ];

            $users = [
                "skhanker", "sean", "kevinho"
            ];

            $messages = [
                "jus workin it like the rent is due",
                "you go ingester, parse that body, four for you, you can do this",
                "open collections is coming",
                "to be or not to be (parsed), that is the question",
                "aint even bovvered",
                "art thou calling me a goodly rotten apple?!",
                "can't win them all, now can you"
            ];

            $teaser = "";
            if ($sendTeaser) {
                $teaser = " | @" . $users[rand (0, count ($users) - 1)] . " " . $messages[rand (0, count ($messages) - 1)];
            }


            $room = ($room) ? $room : "open-collections";
            $data = "payload=" . json_encode (
                    [
                        "channel"    => "#{$room}",
                        "text"       => "{$message}{$teaser}",
                        "icon_emoji" => $icon
                    ]
                );

            // You can get your webhook endpoint from your Slack settings
            $ch = curl_init ("https://hooks.slack.com/services/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");
            curl_setopt ($ch, CURLOPT_CUSTOMREQUEST, "POST");
            curl_setopt ($ch, CURLOPT_POSTFIELDS, $data);
            curl_setopt ($ch, CURLOPT_RETURNTRANSFER, true);
            $result = curl_exec ($ch);
            curl_close ($ch);

            return $result;
        }

        public function rodGetWordPositions ($filePath, $w, $h)
        {
            $fh = fopen ($filePath, 'r');
            $bug_fix = false;
            while ($line = fgets ($fh)) {
                if (preg_match ('/\t.*?:-/', $line)) {
                    $bug_fix = true;
                    break;
                }
                if (preg_match ('/\t.*?[0-9]{6}/', $line)) {
                    $bug_fix = true;
                    break;
                }
                if (preg_match ('/\t.*?[789][0-9]{5}/', $line)) {
                    $bug_fix = true;
                    break;
                }
            }
            if ($bug_fix) {
                echo "BUG FIX" . CTEN;
            }
            rewind ($fh);
            $data = [];
            while ($line = fgets ($fh)) {
                preg_match ("/^([^\t]*)\t(.*$)/", $line, $e);
                if (count ($e) != 3) {
                    echo "Problem: $filePath: " . $e[0] . "\n";
                } else {
                    $boxes = explode (' ', trim ($e[2]));
                    $positions = [];
                    foreach ($boxes as $box) {
                        $xywh = explode (':', $box);
                        if ($bug_fix) {
                            $xywh[0] = 0xFFFF & ($xywh[0] >> 1);
                            if ($xywh[2] < 0) {
                                $xywh[2] = 350 * strlen ($e[1]); // width information is lost, so welp
                            } else {
                                $xywh[2] = 0x0FFF & ($xywh[2] >> 1);
                            }
                        }
                        $x1 = floor ($w * $xywh[0]) >> 16;
                        $y1 = floor ($h * $xywh[1]) >> 16;
                        $wi = floor ($w * $xywh[2]) >> 16;
                        $he = floor ($h * $xywh[3]) >> 16;
                        $positions[] = [
                            'x' => $x1, 'y' => $y1, 'w' => $wi, 'h' => $he
                        ];
                    }
                    $tmpstr = str_replace ('\\', '\\\\', $e[1]);
                    $str = str_replace ('"', '\\"', $tmpstr);
                    preg_replace ("/\\\\/mi", "\\$0", $str);
                    $data[] = ['word' => trim ($str), 'coordinates' => $positions];
                }
            }
            fclose ($fh);

            return $data;
        }


        public function flattenPages ($handle, $node, $p, $nodetitle)
        {
            if (isset($node[0])) {
                foreach ($node as $subnode) {
                    $nodetitle[] = $subnode['nodetitle'];
                    if ( !empty($subnode['page'])) {
                        foreach ($subnode['page'] as $i => $snp) {
                            $subnode['page'][$i]['nodetitle'] = implode (' - ', $nodetitle);
                        }
                        $p = array_merge ($p, $subnode['page']);
                    }
                    if (isset($subnode['node'])) {
                        $p = $this->flattenPages ($handle, $subnode['node'], $p, $nodetitle);
                    }
                    array_pop ($nodetitle);
                }
            } else {
                if ( !empty($node['nodetitle'])) {
                    $nodetitle[] = $node['nodetitle'];
                }
                if (isset($node['page'])) {
                    foreach ($node['page'] as $i => $snp) {
                        $node['page'][$i]['nodetitle'] = implode (' - ', $nodetitle);
                    }
                    $p = array_merge ($p, $node['page']);
                }
                if (isset($node['node'])) {
                    $p = $this->flattenPages ($handle, $node['node'], $p, $nodetitle);
                }
                if (isset($node['nodetitle'])) {
                    array_pop ($nodetitle);
                }
            }

            return $p;
        }

        function mapValuesToFields (&$getValuesFrom, $setValuesIn, $isChild = false)
        {
            foreach ($setValuesIn as $ubcMappingField => &$sourceFieldsToMapAgainst) {
                if ($ubcMappingField == "__contentdm") {//todo skk fix this
                    foreach ($sourceFieldsToMapAgainst as &$contentDmSystemField) {
                        $contentDmSystemField[0]['value'] = $getValuesFrom[$contentDmSystemField[0]['key']];
                    }
                    continue;
                }
                echo "  -- Mapping Key: {$ubcMappingField}\n";
                $unset = true;
                foreach ($sourceFieldsToMapAgainst as &$fieldToSetValueUsingGetValueFrom) {
                    $key = $fieldToSetValueUsingGetValueFrom['key'];
                    if (isset ($getValuesFrom[$key])) {
                        //remember, the final "value" that you get from CDM could still be an array (string separated by <br><br> etc)
                        if (is_array ($getValuesFrom[$key])) {
                            foreach ($getValuesFrom[$key] as $explodeMe) {
                                $temprrr = $this->splitValues ("{$ubcMappingField}", $getValuesFrom[$key]);//need to split a string that might be denoted as multi- (using <br><br>)
                                foreach ($temprrr as $pushMe) {
                                    array_push ($fieldToSetValueUsingGetValueFrom['value'], $pushMe);
                                }
                            }
                        } else {
                            $fieldToSetValueUsingGetValueFrom['value'] = $this->splitValues ("{$ubcMappingField}", $getValuesFrom[$key]);
                        }
                        if ( !empty($fieldToSetValueUsingGetValueFrom['value'])) {
                            $unset = false;
                        }
                    } else {
                        echo "  -- ERROR - could not find an entry for \$getValuesFrom[{$key}]\n\n";
                    }
                }
                if ($unset && $isChild) {
                    unset($setValuesIn[$ubcMappingField]);
                }
            }

            return $setValuesIn;
        }

        public function splitValues ($title, $value)
        {
            switch ($title) {
                case 'dc.title':
                case 'dc.title.alternative':
                case 'dc.publisher':
                case 'dc.format.extent':
                    $value = preg_split ('/(<br.?' . '>)+/i', $value);
                    break;
                case 'dc.contributor.author':
                case 'dc.creator':
                case 'dc.subject':
                case 'dc.subject.geographic':
                case 'dc.subject.hasPersonalNames':
                case 'dc.genre':
                case 'dc.type':
                case 'dc.format':
                case 'dc.identifier':
                case 'dc.identifier.callnumber':
                case 'dc.identifier.accession':
                    $value = preg_split ('/\s*;\s*/', $value);
                    break;
                case 'dc.description':
                case 'dc.description.note':
                    $value = preg_replace ('/<br.?' . '>/', "\n", $value);
                    $value = str_replace ("\n\n", "\n", $value);
                    break;
            }

            if (is_string ($value)) {
                echo "  -- Forcing {$title} to array\n";
                $value = explode ('THISISADELIMITERTHATSHOULDNEVEREXIST1234567890', $value);
            }

            return $value;
        }

        public function getValueFromArrayOfArrays (&$arrayOfArrays, $needle)
        {
            $ret = [];
            foreach ($arrayOfArrays as $array) {
                $key = strtolower (str_replace (" ", "", $array['label']));
                if (stripos ($key, $needle) !== false) {
                    foreach ($array['value'] as $pushThis) {
                        $ret [] = $pushThis;
                    }
                }
            }

            return $ret;
        }

        public function arrayOfArraysToArray (&$arrayOfArrays)
        {
            $ret = [];
            foreach ($arrayOfArrays as $array) {
                if (is_array ($array['value'])) {
                    foreach ($array['value'] as $pushThis) {
                        $ret [] = $pushThis;
                    }
                } else {
                    $ret[] = $array['value'];
                }
            }

            return $ret;
        }


        public function parseImageInfo (&$arr, &$imgInfo)
        {
            $arr['ubc.image.filename'] = isset($imgInfo['filename']) ? $imgInfo['filename'] : '';
            $arr['ubc.image.height'] = isset($imgInfo['height']) ? $imgInfo['height'] : -1;
            $arr['ubc.image.width'] = isset($imgInfo['width']) ? $imgInfo['width'] : -1;
            $arr['ubc.image.type'] = isset($imgInfo['type']) ? $imgInfo['type'] : '';
            $arr['ubc.image.title'] = isset($imgInfo['title']) ? $imgInfo['title'] : '';
        }

        private function returnJSON ($json)
        {
            header ('Content-Type: application/json');
            echo $json;
            exit;
        }

        public function cronDeleteMulti ()
        {
            // find all indices and their parameters
            $allCollections = $this->getCollections ();
            # echo json_encode ($allCollections);
            echo "\n";
            echo "-------------------------------------------------------\n";
            echo "-- Triggering deleted items in:\n";
            $i = 0;
            foreach ($allCollections as $index => $params) {
                $i++;
                # $collectionName = $params['name'];
                $collectionPath = $params['path'];
                $filename = __DIR__ . '/out/cdm.' . $index . '.delete.log';
                # exec("touch {$filename}",$res);
                $cmd = "nice -10 php ./harvest.php --cron-delete";
                $cmd .= " $index $collectionPath > {$filename} 2>&1 &";
                echo ("- $cmd\n");
                exec ($cmd, $res);
            }
            echo "Spawned {$i} Delete Sub Tasks.\n";

            return true;
        }

        public function cronDelete ($index, $collectionPath)
        {
            echo "Triggering Delete Scan: {$index}[$collectionPath]\n";
            $time_start = microtime (true);
            echo "Starting Scan - " . time () . "\n";
            $this->remoteDeleteExistingItems ($index, $collectionPath);
            echo "Finished Scan - " . time () . "\n";
            $time_stop = microtime (true);
            echo "Scan Finished in " . $time_stop - $time_start . "s\n";
        }
    }