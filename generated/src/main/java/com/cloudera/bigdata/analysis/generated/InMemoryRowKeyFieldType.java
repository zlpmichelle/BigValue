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
 * <p>Java class for InMemoryRowKeyFieldType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="InMemoryRowKeyFieldType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="fieldType" type="{}FieldType"/>
 *         &lt;element name="fixedLength" type="{http://www.w3.org/2001/XMLSchema}boolean"/>
 *         &lt;element name="length" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *         &lt;element name="fieldPattern" type="{}FieldPattern"/>
 *         &lt;element name="randomAlgorithm" type="{}RandomAlgorithm"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "InMemoryRowKeyFieldType", propOrder = {
    "fieldType",
    "fixedLength",
    "length",
    "fieldPattern",
    "randomAlgorithm"
})
public class InMemoryRowKeyFieldType {

    @XmlElement(required = true)
    protected FieldType fieldType;
    protected boolean fixedLength;
    protected int length;
    @XmlElement(required = true)
    protected FieldPattern fieldPattern;
    @XmlElement(required = true)
    protected RandomAlgorithm randomAlgorithm;

    /**
     * Gets the value of the fieldType property.
     * 
     * @return
     *     possible object is
     *     {@link FieldType }
     *     
     */
    public FieldType getFieldType() {
        return fieldType;
    }

    /**
     * Sets the value of the fieldType property.
     * 
     * @param value
     *     allowed object is
     *     {@link FieldType }
     *     
     */
    public void setFieldType(FieldType value) {
        this.fieldType = value;
    }

    /**
     * Gets the value of the fixedLength property.
     * 
     */
    public boolean isFixedLength() {
        return fixedLength;
    }

    /**
     * Sets the value of the fixedLength property.
     * 
     */
    public void setFixedLength(boolean value) {
        this.fixedLength = value;
    }

    /**
     * Gets the value of the length property.
     * 
     */
    public int getLength() {
        return length;
    }

    /**
     * Sets the value of the length property.
     * 
     */
    public void setLength(int value) {
        this.length = value;
    }

    /**
     * Gets the value of the fieldPattern property.
     * 
     * @return
     *     possible object is
     *     {@link FieldPattern }
     *     
     */
    public FieldPattern getFieldPattern() {
        return fieldPattern;
    }

    /**
     * Sets the value of the fieldPattern property.
     * 
     * @param value
     *     allowed object is
     *     {@link FieldPattern }
     *     
     */
    public void setFieldPattern(FieldPattern value) {
        this.fieldPattern = value;
    }

    /**
     * Gets the value of the randomAlgorithm property.
     * 
     * @return
     *     possible object is
     *     {@link RandomAlgorithm }
     *     
     */
    public RandomAlgorithm getRandomAlgorithm() {
        return randomAlgorithm;
    }

    /**
     * Sets the value of the randomAlgorithm property.
     * 
     * @param value
     *     allowed object is
     *     {@link RandomAlgorithm }
     *     
     */
    public void setRandomAlgorithm(RandomAlgorithm value) {
        this.randomAlgorithm = value;
    }

}
