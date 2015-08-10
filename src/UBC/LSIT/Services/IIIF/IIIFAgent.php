<?php
    /**
     * Created by PhpStorm.
     * User: hajime
     * Date: 09 Aug 15
     * Time: 14:41
     */
    
    namespace UBC\LSIT\Services\IIIF;

    class IIIFAgent {

        private $serverURI;

        private $imageAPI;

        private $presentationAPI;

        /**
         * IIIFAgent constructor.
         */
        public function __construct () {

            $conf                  = parse_ini_file('config.ini');
            $this->serverURI       = $conf['server_uri'];
            $this->imageAPI        = $conf['server_uri'];
            $this->presentationAPI = $conf['server_uri'];
        }

        public function generateThumbnails (&$r, $thumbnailSize = 400, $sectionAsXYWH = "full") {

            echo " IIIFAgent: GET /\n";

            $i = 0;
            $b = 1;//batch
            foreach ($r as $handle => $vBool) {
                //TODO SKK technically not todo, but you could check if the item added successfully here and set vbool=false if item not found, as iiif needs item to exist
                if($vBool) {
                    $res = $this->generate($handle, $thumbnailSize, $sectionAsXYWH);
                    $i++;
                    if($i % 25 === 0) {
                        echo "                 Sent {$i} request to IIIFAgent [batch #{$b}], waiting 10s to allow recovery\n";
                        $b++;
                        sleep(10);//added two seconds to allow user to read above echo
                    }
                }
            }
        }

        public function generateThumbnail ($handle, $thumbnailSize = 400, $sectionAsXYWH = "full") {

            echo " IIIFAgent: GET /\n";
            $res = $this->generate($handle, $thumbnailSize, $sectionAsXYWH);
        }

        private function generate ($handle, $thumbnailSize = 400, $sectionAsXYWH = "full") {

            $imgHandle = urlencode($handle);
            echo "                 {$this->serverURI}{$this->imageAPI}/{$imgHandle}/{$sectionAsXYWH}/{$thumbnailSize},/0/default.jpg?nice&nocache > /dev/null 2>&1 &\n";
            $res = exec("curl --silent --max-time 1 -XHEAD {$this->serverURI}{$this->imageAPI}/{$imgHandle}/{$sectionAsXYWH}/{$thumbnailSize},/0/default.jpg?nice&nocache > /dev/null 2>&1 &");

            return $res;
        }
    }