//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.7 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2014.11.11 at 05:32:40 AM UTC 
//


package com.cloudera.bigdata.analysis.generated;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for ColumnSpec complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="ColumnSpec">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="name" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="start" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="end" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="value" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="length" type="{http://www.w3.org/2001/XMLSchema}long"/>
 *         &lt;element name="pattern" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="udfName" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="ref" type="{}ColumnRef"/>
 *         &lt;element name="randomPattern" type="{}RandomPattern"/>
 *         &lt;element name="randomType" type="{}RandomType"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ColumnSpec", propOrder = {
    "name",
    "start",
    "end",
    "value",
    "length",
    "pattern",
    "udfName",
    "ref",
    "randomPattern",
    "randomType"
})
public class ColumnSpec {

    @XmlElement(required = true)
    protected String name;
    @XmlElement(required = true)
    protected String start;
    @XmlElement(required = true)
    protected String end;
    @XmlElement(required = true)
    protected String value;
    protected long length;
    @XmlElement(required = true)
    protected String pattern;
    @XmlElement(required = true)
    protected String udfName;
    @XmlElement(required = true)
    protected ColumnRef ref;
    @XmlElement(required = true)
    protected RandomPattern randomPattern;
    @XmlElement(required = true)
    protected RandomType randomType;

    /**
     * Gets the value of the name property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the value of the name property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setName(String value) {
        this.name = value;
    }

    /**
     * Gets the value of the start property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getStart() {
        return start;
    }

    /**
     * Sets the value of the start property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setStart(String value) {
        this.start = value;
    }

    /**
     * Gets the value of the end property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getEnd() {
        return end;
    }

    /**
     * Sets the value of the end property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setEnd(String value) {
        this.end = value;
    }

    /**
     * Gets the value of the value property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getValue() {
        return value;
    }

    /**
     * Sets the value of the value property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Gets the value of the length property.
     * 
     */
    public long getLength() {
        return length;
    }

    /**
     * Sets the value of the length property.
     * 
     */
    public void setLength(long value) {
        this.length = value;
    }

    /**
     * Gets the value of the pattern property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getPattern() {
        return pattern;
    }

    /**
     * Sets the value of the pattern property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setPattern(String value) {
        this.pattern = value;
    }

    /**
     * Gets the value of the udfName property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUdfName() {
        return udfName;
    }

    /**
     * Sets the value of the udfName property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUdfName(String value) {
        this.udfName = value;
    }

    /**
     * Gets the value of the ref property.
     * 
     * @return
     *     possible object is
     *     {@link ColumnRef }
     *     
     */
    public ColumnRef getRef() {
        return ref;
    }

    /**
     * Sets the value of the ref property.
     * 
     * @param value
     *     allowed object is
     *     {@link ColumnRef }
     *     
     */
    public void setRef(ColumnRef value) {
        this.ref = value;
    }

    /**
     * Gets the value of the randomPattern property.
     * 
     * @return
     *     possible object is
     *     {@link RandomPattern }
     *     
     */
    public RandomPattern getRandomPattern() {
        return randomPattern;
    }

    /**
     * Sets the value of the randomPattern property.
     * 
     * @param value
     *     allowed object is
     *     {@link RandomPattern }
     *     
     */
    public void setRandomPattern(RandomPattern value) {
        this.randomPattern = value;
    }

    /**
     * Gets the value of the randomType property.
     * 
     * @return
     *     possible object is
     *     {@link RandomType }
     *     
     */
    public RandomType getRandomType() {
        return randomType;
    }

    /**
     * Sets the value of the randomType property.
     * 
     * @param value
     *     allowed object is
     *     {@link RandomType }
     *     
     */
    public void setRandomType(RandomType value) {
        this.randomType = value;
    }

}
