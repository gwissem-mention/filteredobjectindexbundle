<?php
namespace Celltrak\FilteredObjetIndexBundle\Tests;

use Celltrak\RedisBundle\Component\Client\CelltrakRedis;


class FilteredObjectIndexTestCase extends \PHPUnit_Framework_TestCase
{

    const PLANETS = [
        'mercury',
        'venus',
        'earth',
        'mars',
        'jupiter',
        'saturn',
        'uranus',
        'neptune'
    ];

    const ASTRONOMICAL_BODIES = [
        'mercury',
        'venus',
        'earth',
        'mars',
        'jupiter',
        'saturn',
        'uranus',
        'neptune',
        'pluto',
        'sun',
        'moon',
        'europa',
        'io',
        'ganymede'
    ];

    const ROMAN_GODS = [
        'mercury',
        'venus',
        'mars',
        'jupiter',
        'saturn',
        'pluto',
        'vulcan',
        'apollo'
    ];


    protected function setUp()
    {
        $this->redis = new CelltrakRedis();
        $this->redis->connect('localhost');
        $this->redis->auth('celltrak');
        $this->redis->select(16);

        $loggerMock = $this->getMockBuilder(Logger::class)
            ->disableOriginalConstructor()
            ->getMock();
    }

    protected function tearDown()
    {
        $this->redis->flushDb();
        $this->redis->close();
    }

    protected function assertEqualArrayValues(array $a1, array $a2)
    {
        $this->assertEquals(count($a1), count($a2));
        $diff = array_diff($a1, $a2);
        $this->assertCount(0, $diff);
    }

    protected function getUniqueValues(array ...$arrays)
    {
        $mergedValues = array_merge(...$arrays);
        return array_unique($mergedValues);
    }

    protected function getUniqueValueCount(array ...$arrays)
    {
        $uniqueValues = $this->getUniqueValues(...$arrays);
        return count($uniqueValues);
    }

    protected function getUniqueSharedValues(array ...$arrays)
    {
        return array_intersect(...$arrays);
    }

    protected function getUniqueSharedValueCount(array ...$arrays)
    {
        $values = $this->getUniqueSharedValues(...$arrays);
        return count($values);
    }




}
