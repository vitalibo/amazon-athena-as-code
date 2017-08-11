ALTER DATABASE ${Name}
SET DBPROPERTIES (
    <#list Properties as Property>
    '${Property.Name}'='${Property.Value}'<#if Property_has_next>,</#if>
    </#list>
);