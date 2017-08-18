CREATE DATABASE `${Name}`
<#if Comment??>
COMMENT '${Comment}'
</#if>
<#if Location??>
LOCATION '${Location}'
</#if>
<#if Properties??>
WITH DBPROPERTIES (
    <#list Properties as Property>
    '${Property.Name}'='${Property.Value}'<#if Property_has_next>,</#if>
    </#list>
)
</#if>
;