<?php
namespace Celltrak\FilteredObjectIndexBundle\Tests\Query;

use Celltrak\FilteredObjectIndexBundle\Component\Query\GroupedFilterQuery;


class GroupedFilterQueryTest extends \PHPUnit_Framework_TestCase
{

    public function testCreateGroupedFilterQuery()
    {
        $query = new GroupedFilterQuery;

        $this->assertEquals(0, $query->getGroupCount());
        $this->assertCount(0, $query->getGroupedFilters());
    }

    public function testAddSingleSetOfGroupedFilters()
    {
        $query = new GroupedFilterQuery;
        $query->addGroupedFilters('dinner', 'dessert');

        $this->assertEquals(1, $query->getGroupCount());
        $this->assertEquals(
            ['dinner', 'dessert'],
            $query->getGroupedFilters()[0]
        );
    }

    public function testAddMultipleSetsOfGroupedFilters()
    {
        $query = new GroupedFilterQuery;
        $query->addGroupedFilters('dinner', 'dessert');
        $query->addGroupedFilters('illinois');

        $this->assertEquals(2, $query->getGroupCount());
        $this->assertEquals(
            ['dinner', 'dessert'],
            $query->getGroupedFilters()[0]
        );
        $this->assertEquals(
            ['illinois'],
            $query->getGroupedFilters()[1]
        );
    }

    public function testIterator()
    {
        $query = new GroupedFilterQuery;
        $query->addGroupedFilters('dinner', 'dessert');
        $query->addGroupedFilters('illinois');

        foreach ($query as $i => $filters) {
            if ($i == 0) {
                $this->assertEquals(['dinner', 'dessert'], $filters);
            } else {
                $this->assertEquals(['illinois'], $filters);
            }
        }
    }


}
