//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.7 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2016.01.11 at 02:18:24 AM PST 
//


package com.cloudera.bigdata.analysis.generated;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for ColumnRef complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="ColumnRef">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="refColumn" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="refStart" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *         &lt;element name="refEnd" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *         &lt;element name="refOp" type="{}OpType"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ColumnRef", propOrder = {
    "refColumn",
    "refStart",
    "refEnd",
    "refOp"
})
public class ColumnRef {

    @XmlElement(required = true)
    protected String refColumn;
    protected int refStart;
    protected int refEnd;
    @XmlElement(required = true)
    protected OpType refOp;

    /**
     * Gets the value of the refColumn property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getRefColumn() {
        return refColumn;
    }

    /**
     * Sets the value of the refColumn property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setRefColumn(String value) {
        this.refColumn = value;
    }

    /**
     * Gets the value of the refStart property.
     * 
     */
    public int getRefStart() {
        return refStart;
    }

    /**
     * Sets the value of the refStart property.
     * 
     */
    public void setRefStart(int value) {
        this.refStart = value;
    }

    /**
     * Gets the value of the refEnd property.
     * 
     */
    public int getRefEnd() {
        return refEnd;
    }

    /**
     * Sets the value of the refEnd property.
     * 
     */
    public void setRefEnd(int value) {
        this.refEnd = value;
    }

    /**
     * Gets the value of the refOp property.
     * 
     * @return
     *     possible object is
     *     {@link OpType }
     *     
     */
    public OpType getRefOp() {
        return refOp;
    }

    /**
     * Sets the value of the refOp property.
     * 
     * @param value
     *     allowed object is
     *     {@link OpType }
     *     
     */
    public void setRefOp(OpType value) {
        this.refOp = value;
    }

}
