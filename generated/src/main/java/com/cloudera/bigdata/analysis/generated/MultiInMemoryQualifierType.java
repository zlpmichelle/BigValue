//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.7 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2016.01.12 at 11:10:56 PM PST 
//


package com.cloudera.bigdata.analysis.generated;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for MultiInMemoryQualifierType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="MultiInMemoryQualifierType">
 *   &lt;complexContent>
 *     &lt;extension base="{}InMemoryQualifierType">
 *       &lt;sequence>
 *         &lt;element name="qualifierNum" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *         &lt;element name="qualifierPrefix" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="useConstant" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *         &lt;element name="constantValue" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "MultiInMemoryQualifierType", propOrder = {
    "qualifierNum",
    "qualifierPrefix",
    "useConstant",
    "constantValue"
})
public class MultiInMemoryQualifierType
    extends InMemoryQualifierType
{

    protected int qualifierNum;
    @XmlElement(required = true)
    protected String qualifierPrefix;
    @XmlElement(defaultValue = "false")
    protected boolean useConstant;
    @XmlElement(required = true)
    protected String constantValue;

    /**
     * Gets the value of the qualifierNum property.
     * 
     */
    public int getQualifierNum() {
        return qualifierNum;
    }

    /**
     * Sets the value of the qualifierNum property.
     * 
     */
    public void setQualifierNum(int value) {
        this.qualifierNum = value;
    }

    /**
     * Gets the value of the qualifierPrefix property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getQualifierPrefix() {
        return qualifierPrefix;
    }

    /**
     * Sets the value of the qualifierPrefix property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setQualifierPrefix(String value) {
        this.qualifierPrefix = value;
    }

    /**
     * Gets the value of the useConstant property.
     * 
     */
    public boolean isUseConstant() {
        return useConstant;
    }

    /**
     * Sets the value of the useConstant property.
     * 
     */
    public void setUseConstant(boolean value) {
        this.useConstant = value;
    }

    /**
     * Gets the value of the constantValue property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getConstantValue() {
        return constantValue;
    }

    /**
     * Sets the value of the constantValue property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setConstantValue(String value) {
        this.constantValue = value;
    }

}
