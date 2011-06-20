<?php

namespace li3_delayed_job\models;

use lithium\analysis\Logger;

class Workers {
  const SLEEP = 5;
  
  protected $quiet = false;
  
  public function __construct(array $config = array()) {
    $config += array('quiet' => false, 'minPriority' => null, 'maxPriority' => null);
    $this->quiet = $config['quiet'];
    
    Jobs::$minPriority = $config['minPriority'];
    Jobs::$maxPriority = $config['maxPriority'];
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
          $this->say(sprintf($count.' jobs processed at %.4f j/s, %d failed ...', $count/$realtime, $result['failure']));
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