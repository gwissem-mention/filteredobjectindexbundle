<?php
namespace Celltrak\FilteredObjectIndexBundle\DependencyInjection;

use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;


class CelltrakFilteredObjectIndexConfiguration implements ConfigurationInterface
{
    public function getConfigTreeBuilder()
    {
        $tb = new TreeBuilder();
        $root = $tb->root('celltrak_filtered_object_index');

        $root
            ->children()
                ->scalarNode('tenant_namespace')
                    ->info('Prevent collisions in key namespace when used in multi-tenant setup')
                ->end()
                ->arrayNode('index_groups')
                    ->info('Defines index groups')
                    ->isRequired()
                    ->requiresAtLeastOneElement()
                    ->useAttributeAskey('indexGroupName')
                    ->prototype('array')
                        ->children()
                            ->scalarNode('redis_client')
                                ->info('Service ID of Redis client')
                                ->isRequired()
                                ->cannotBeNull()
                            ->end()
                            ->integerNode('object_lock_ttl')
                                ->info('Number of seconds to hold an object lock during write operations')
                                ->defaultValue(10)
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end()
        ->end();

        return $tb;
    }



}
