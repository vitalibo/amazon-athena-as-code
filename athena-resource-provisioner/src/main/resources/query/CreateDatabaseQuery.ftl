CREATE DATABASE IF NOT EXISTS ${Name}
LOCATION 's3://${Location}/'
<#if Properties??>
WITH DBPROPERTIES (
    <#list Properties as Property>
    '${Property.Name}'='${Property.Value}'<#if Property_has_next>,</#if>
    </#list>
);
</#if>
