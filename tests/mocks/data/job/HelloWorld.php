<?php

namespace li3_delayed_job\tests\mocks\data\job;

class HelloWorld
{
  public function perform() {
    $var = 'Hello World!';
  }
}