<?php
    
    namespace UBC\LSIT\Harvester\CONTENTdm;

    use PDO;

    define('DEBUG', false);

    /**
     * The DB class adds some convenience methods to the PHP PDO class
     */
    class DB extends PDO {

        private $transactionLevel = 0;

        /**
         * Collects prepared PDO statements
         *
         * @var array
         */
        private $_prep = [];

        /**
         * Constructor -- pass args to parent PDO object, then set
         * fetch mode to PDO::ASSOC
         */
        function __construct () {

            $args    = func_get_args();
            $args [] = [
                PDO::ATTR_PERSISTENT => true
            ];
            call_user_func_array([
                $this,
                'parent::__construct'
            ], $args);
            $this->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
        }

        function __destruct () {

            while ($this->transactionLevel > 0) {
                error_log('DB::__destruct rolling back transaction level ' . $this->transactionLevel);
                $this->rollBack();
            }
        }

        public function beginTransaction () {

            if($this->transactionLevel === 0) {
                $this->setAttribute(PDO::ATTR_AUTOCOMMIT, false);
                $this->transactionLevel++;

                return parent::beginTransaction();
            }
            else {
                $this->exec('SAVEPOINT LEVEL' . $this->transactionLevel);
                $this->transactionLevel++;
            }

            return 1;
        }

        public function rollBack () {

            $this->transactionLevel--;
            if($this->transactionLevel === 0) {
                $this->setAttribute(PDO::ATTR_AUTOCOMMIT, true);

                return parent::rollBack();
            }
            else {
                $this->exec('ROLLBACK TO SAVEPOINT LEVEL' . $this->transactionLevel);

                return 1;
            }
        }

        public function commit () {

            // $db=debug_backtrace();error_log( $db[0]['file'].' : '.$db[0]['line']." commit");
            $this->transactionLevel--;
            if($this->transactionLevel === 0) {
                $r = parent::commit();
                $this->setAttribute(PDO::ATTR_AUTOCOMMIT, true);

                return $r;
            }
            else {
                $this->exec('RELEASE SAVEPOINT LEVEL' . $this->transactionLevel);

                return 1;
            }
        }

        private function array_recode ($fromto, $input) {

            if(!is_array($input)) {
                $uns = @unserialize($input);
                if(is_array($uns)) {
                    $uns = $this->array_recode($fromto, $uns);

                    return serialize($uns);
                }
                else {
                    $tmp = @json_encode($input);
                    $e   = json_last_error();
                    if($e) {
                        $fix = recode($fromto, $input);

                        // error_log("UTF8 fix from [$input] to [$fix]");
                        return $fix;
                    }
                    else {
                        return $input;
                    }
                }
            }
            else {
                foreach ($input as $i => $v) {
                    $input [ $i ] = $this->array_recode($fromto, $v);
                }

                return $input;
            }
        }

        /**
         * Prepres and stores SQL query if we haven't seen it before,
         * then executes the query and returns the result as a PDOStatement.
         *
         * Note $bind may be null, a simple type, or an array
         *
         * @param string  $sql
         * @param mixed   $bind
         * @param boolean $die_on_error
         *
         * @return \PDOStatement
         */
        public function execute ($sql, $bind = null, $die_on_error = true) {

            if(strpos($sql, '--') === false) {
                $sql = trim(preg_replace('/\s+/', ' ', $sql));
            }
            if(!isset ($this->_prep [ $sql ])) {
                $this->_prep [ $sql ] = $this->prepare($sql);
            }
            $stmt  = $this->_prep [ $sql ];
            $timer = microtime(true);
            if(is_null($bind)) {
                $stmt->execute();
            }
            else {
                $bind = $this->array_recode('Latin1..UTF8', $bind);
                if(!is_array($bind)) {
                    $bind = [
                        $bind
                    ];
                }
                // var_export($bind);
                $stmt->execute($bind);
            }
            $timer = microtime(true) - $timer;
            if($timer > 0.8) {
                error_log("Slow query ($timer s): $sql\nBind: " . serialize($bind));
            }
            if(DEBUG) {
                echo '<div class="debug"><pre>' . $sql . '</pre>';
                if(!is_null($bind)) {
                    echo '<div class="bind">Bind vars: ';
                    foreach ($bind as $bv => $b) {
                        echo '<span>' . $bv . ' => ' . htmlspecialchars($b) . '</span>';
                    }
                    echo '</div>';
                }
                $first = true;
                if($stmt->rowCount() > 0) {
                    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
                        if($first) {
                            $first = false;
                            echo '<table><thead><tr><th>';
                            echo implode('</th><th>', array_keys($row));
                            echo '</th></thead><tbody>';
                        }
                        echo '<tr>';
                        foreach ($row as $val) {
                            echo '<td>' . htmlspecialchars($val) . '</td>';
                        }
                        echo '</tr>';
                    }
                    echo '</tbody></table>';
                }
                else {
                    echo '<p><em>No result</em></p>';
                }
                if($stmt->errorCode() != '00000') {
                    $err = $stmt->errorInfo();
                    echo '<pre>' . implode("\n", $err) . '</pre>';
                    if($die_on_error) {
                        die ();
                    }
                }
                echo '</div>';
                if(is_null($bind)) {
                    $stmt->execute();
                }
                else {
                    if(is_array($bind)) {
                        $stmt->execute($bind);
                    }
                    else {
                        $stmt->execute([
                            $bind
                        ]);
                    }
                }
            }
            elseif($die_on_error && ($stmt->errorCode() != '00000')) {
                $fh  = fopen(dirname(__FILE__) . '/dberr.txt', 'a');
                $err = $stmt->errorInfo();
                fwrite($fh, date('Y-m-d H:i:s') . "\n" . $sql . "\n" . implode("\n", $err) . "\n\n");
                if($bind) {
                    fwrite($fh, "Bind:\n" . var_export($bind, true));
                }
                $db = debug_backtrace();
                error_log($db [0] ['file'] . ' : ' . $db [0] ['line'] . " DB ERROR");
                fclose($fh);
                exit ('Database error, check dberr.txt');
            }

            return $stmt;
        }

        /**
         * Return one result from a query
         *
         * @param string $sql
         * @param mixed  $bind
         *
         * @return array
         */
        public function queryOneRow ($sql, $bind = null) {

            $res = $this->execute($sql, $bind);

            return $res->fetch();
        }

        /**
         * Return all rows from query in an array
         *
         * @param string $sql
         * @param mixed  $bind
         *
         * @return array
         */
        public function queryRows ($sql, $bind = null) {

            $res = $this->execute($sql, $bind);

            return $res->fetchAll();
        }

        /**
         * Return only one value
         * e.g.
         * queryOneVal('SELECT `name` FROM `user` WHERE `user_id`=?',1)
         * returns string 'Admin'
         *
         * @param string $sql
         * @param mixed  $bind
         *
         * @return string
         */
        public function queryOneVal ($sql, $bind = null) {

            $res = $this->execute($sql, $bind);
            $res = $res->fetch(PDO::FETCH_NUM);

            return $res [0];
        }

        /**
         * Take a column of results and implode them on a glue character
         *
         * @param string $sql
         * @param mixed  $bind
         * @param string $glue
         *
         * @return string
         */
        public function queryImplode ($sql, $bind = null, $glue = ',') {

            $res = $this->execute($sql, $bind);
            $out = [];
            while ($line = $res->fetch(PDO::FETCH_NUM)) {
                $out [] = $line [0];
            }

            return implode($glue, $out);
        }

        /**
         * Organize resultset into an associative array using the indexCol
         * for key values
         *
         * @param string $sql
         * @param mixed  $bind
         *
         * @return array
         */
        public function queryAssoc ($sql, $indexCol, $bind = null) {

            // use first column as index
            $res = $this->execute($sql, $bind);
            $out = [];
            while ($line = $res->fetch()) {
                $index = $line [ $indexCol ];
                unset ($line [ $indexCol ]);
                $out [ $index ] = $line;
            }

            return $out;
        }

        public function queryOneColumn ($sql, $bind = null) {

            $res = $this->execute($sql, $bind);
            $out = $res->fetchAll(PDO::FETCH_COLUMN, 0);

            return $out;
        }

        public function likeQuote ($str, $lr = 'LR') {
            $str = trim($this->quote($str, PDO::PARAM_STR), "'");
            $str = str_replace('%', '\%', $str);
            $str = str_replace('_', '\_', $str);
            if($lr == 'LR') {
                return "%$str%";
            }
            if($lr == 'L') {
                return "%$str";
            }
            if($lr == 'R') {
                return "$str%";
            }
            return "%$str%";
        }
    }

