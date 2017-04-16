<?php namespace Dataset;


trait Helper
{
    protected function morphClassName()
    {
        $extendedClass = get_class($this);
        $underScored = $this->inflector->underscore($extendedClass);

        return $this->inflector->pluralize($underScored);
    }

    protected function isMultidimensionalArray($array)
    {
        return (array_values($array) !== $array);
    }
}