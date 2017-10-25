<?php
namespace Celltrak\FilteredObjectIndexBundle\DependencyInjection;

use Symfony\Component\HttpKernel\DependencyInjection\Extension;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\Config\Definition\Processor;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;


class CelltrakFilteredObjectIndexExtension extends Extension
{

    const CLASS_NS = 'Celltrak\FilteredObjectIndexBundle';


    public function load(array $configs, ContainerBuilder $container)
    {
        $processor = new Processor();
        $config = $processor->processConfiguration(
            new CelltrakFilteredObjectIndexConfiguration(),
            $configs
        );

        $tenantNamespace = $config['tenant_namespace'];

        foreach ($config['index_groups'] as $groupName => $groupConfig) {
            $serviceId = "celltrak_filtered_object_index.{$groupName}.group";
            $def = $this->createIndexGroupDefinition(
                $groupName,
                $groupConfig,
                $tenantNamespace
            );
            $container->setDefinition($serviceId, $def);
        }
    }

    protected function createIndexGroupDefinition(
        $groupName,
        array $groupConfig,
        $tenantNamespace = null
    ) {
        $class = self::CLASS_NS . '\Component\Index\FilteredObjectIndexGroup';
        $args = [
            $groupName,
            new Reference($groupConfig['redis_client']),
            $groupConfig['object_lock_ttl'],
            $tenantNamespace
        ];

        return new Definition($class, $args);
    }

}
