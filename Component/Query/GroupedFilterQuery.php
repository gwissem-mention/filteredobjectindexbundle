<?php
namespace Celltrak\FilteredObjectIndexBundle\Component\Query;


class GroupedFilterQuery implements \Iterator
{

    public function __construct()
    {
        $this->groupedFilters = [];
        $this->iteratorPosition = 0;
    }

    public function addGroupedFilters(...$filters)
    {
        $this->groupedFilters[] = $filters;
    }

    public function getGroupCount()
    {
        return count($this->groupedFilters);
    }

    /**
     * {@inheritDoc}
     */
    public function current()
    {
        return $this->groupedFilters[$this->iteratorPosition];
    }

    /**
     * {@inheritDoc}
     */
    public function key()
    {
        return $this->iteratorPosition;
    }

    /**
     * {@inheritDoc}
     */
    public function next()
    {
        ++$this->iteratorPosition;
    }

    /**
     * {@inheritDoc}
     */
    public function rewind()
    {
        $this->iteratorPosition = 0;
    }

    /**
     * {@inheritDoc}
     */
    public function valid()
    {
        return isset($this->groupedFilters[$this->iteratorPosition]);
    }


}
