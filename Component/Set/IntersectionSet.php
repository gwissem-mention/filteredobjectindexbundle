<?php
namespace Celltrak\FilteredObjectIndexBundle\Component\Set;


class IntersectionSet extends CalculatedSet
{

    /**
     * {@inheritDoc}
     */
    protected function getCalculateCommand()
    {
        return 'sInter';
    }

    /**
     * {@inheritDoc}
     */
    protected function getCalculateAndStoreCommand()
    {
        return 'sInterStore';
    }


}
