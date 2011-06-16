<?php

namespace li3_delayed_job\tests\cases\models;

use li3_delayed_job\models\Jobs;
use li3_delayed_job\tests\mocks\data\job\HelloWorld;
use lithium\data\Connections;
use MongoDate;

class JobsTest extends \lithium\test\Unit {
	public function createJob($job = 'HelloWorld', $priority = 0, $runAt = null) {
	  $className = '\\li3_delayed_job\\tests\\mocks\\data\\job\\'.$job;
	  $job = new $className();
	  
	  Jobs::enqueue($job, $priority, $runAt);
	}
	
	public function setUp() {
	  Jobs::remove();
	}

	public function tearDown() {
	  Jobs::$minPriority = null;
	  Jobs::$maxPriority = null;
	  Jobs::remove();
	}
	
	public function testEnqueue() {
	  $this->createJob();
	  
	  $jobs = Jobs::count();

	  $this->assertEqual(1, $jobs, 'Number of jobs in queue');
	}
	
	public function testRun() {
	  $this->createJob();

	  $results = Jobs::workoff();
	  
	  $jobs = Jobs::count();
	  
	  $this->assertEqual(1, $results['success'], 'Number of successful jobs: {:message}');
	  $this->assertEqual(0, $results['failure'], 'Number of failed jobs: {:message}');
	  $this->assertEqual(0, $jobs, 'Number of jobs remaining in queue: {:message}');
	}
	
	public function testNotRunningFutureJobs() {
	  $this->createJob('HelloWorld', 0, time() + 60*60);

	  $results = Jobs::workoff();
	  
	  $jobs = Jobs::count();
	  
	  $this->assertEqual(0, $results['success'], 'Number of successful jobs: {:message}');
	  $this->assertEqual(0, $results['failure'], 'Number of failed jobs: {:message}');
	  $this->assertEqual(1, $jobs, 'Number of jobs remaining in queue: {:message}');
	}
	
	public function testMinPriority() {
	  $this->createJob('HelloWorld', 3);
	  $this->createJob('HelloWorld', 5);

	  Jobs::$minPriority = 4;
	  $results = Jobs::workoff();
	  
	  $jobs = Jobs::count();
	  $this->assertEqual(1, $results['success'], 'Number of successful jobs: {:message}');
	  $this->assertEqual(0, $results['failure'], 'Number of failed jobs: {:message}');
	  $this->assertEqual(1, $jobs, 'Number of jobs remaining in queue: {:message}');
	}
	
	public function testMaxPriority() {
	  $this->createJob('HelloWorld', 3);
	  $this->createJob('HelloWorld', 5);

	  Jobs::$maxPriority = 4;
	  $results = Jobs::workoff();
	  
	  $jobs = Jobs::count();
	  
	  $this->assertEqual(1, $results['success'], 'Number of successful jobs: {:message}');
	  $this->assertEqual(0, $results['failure'], 'Number of failed jobs: {:message}');
	  $this->assertEqual(1, $jobs, 'Number of jobs remaining in queue: {:message}');
	}
}

?>