<?php

namespace li3_delayed_job\models;

use lithium\analysis\Logger;

class Workers {
  const SLEEP = 5;
  
  protected $quiet = false;
  
  public function __construct(array $config = array()) {
    $config += array('quiet' => false, 'min_priority' => null, 'max_priority' => null);
    $this->quiet = $config['quiet'];
    
    Jobs::$min_priority = $config['min_priority'];
    Jobs::$max_priority = $config['max_priority'];
  }
  
  public function start() {
    $this->say('*** Starting job worker');
    
    try {
      while(true) {
        $result = null;
      
        $time_start = microtime(true);
        $result = Jobs::workOff();
        $time_end = microtime(true);
        $realtime = $time_end - $time_start;
      
        $count = array_sum($result);

        if($count == 0) {
          sleep(Workers::SLEEP);
        } else {
          $this->say(sprintf($count.' jobs processed at %.4f j/s, %d failed ...', $count/$realtime, $result['failed']));
        }
      }
    } catch(Exception $e) {
      Jobs::clearLocks();
    }
  }
  
  public function say($text) {
    if($this->quiet == false) {
      echo $text.PHP_EOL;
    }
    Logger::info($text);
  }
}