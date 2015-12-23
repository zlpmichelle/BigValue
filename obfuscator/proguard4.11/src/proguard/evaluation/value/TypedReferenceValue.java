/*
 * ProGuard -- shrinking, optimization, obfuscation, and preverification
 *             of Java bytecode.
 *
 * Copyright (c) 2002-2013 Eric Lafortune (eric@graphics.cornell.edu)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package proguard.evaluation.value;

import proguard.classfile.*;
import proguard.classfile.util.ClassUtil;
import proguard.classfile.visitor.ClassCollector;

import java.util.*;

/**
 * This ReferenceValue represents a partially evaluated reference value.
 * It has a type and a flag that indicates whether the value could be
 * <code>null</code>. If the type is <code>null</code>, the value is
 * <code>null</code>.
 *
 * @author Eric Lafortune
 */
public class TypedReferenceValue extends ReferenceValue
{
    private static final boolean DEBUG = false;


    protected final String  type;
    protected final Clazz   referencedClass;
    protected final boolean mayBeNull;


    /**
     * Creates a new TypedReferenceValue.
     */
    public TypedReferenceValue(String  type,
                               Clazz   referencedClass,
                               boolean mayBeNull)
    {
        this.type            = type;
        this.referencedClass = referencedClass;
        this.mayBeNull       = mayBeNull;
    }


    // Implementations for ReferenceValue.

    public String getType()
    {
        return type;
    }


    public Clazz getReferencedClass()
    {
        return referencedClass;
    }


    // Implementations of unary methods of ReferenceValue.

    public int isNull()
    {
        return type == null ? ALWAYS :
               mayBeNull    ? MAYBE  :
                              NEVER;
    }


    public int instanceOf(String otherType, Clazz otherReferencedClass)
    {
        String thisType = this.type;

        // If this type is null, it is never an instance of any class.
        if (thisType == null)
        {
            return NEVER;
        }

        // Start taking into account the type dimensions.
        int thisDimensionCount   = ClassUtil.internalArrayTypeDimensionCount(thisType);
        int otherDimensionCount  = ClassUtil.internalArrayTypeDimensionCount(otherType);
        int commonDimensionCount = Math.min(thisDimensionCount, otherDimensionCount);

        // Strip any common array prefixes.
        thisType  = thisType.substring(commonDimensionCount);
        otherType = otherType.substring(commonDimensionCount);

        // If either stripped type is a primitive type, we can tell right away.
        if (commonDimensionCount > 0 &&
            (ClassUtil.isInternalPrimitiveType(thisType.charAt(0)) ||
             ClassUtil.isInternalPrimitiveType(otherType.charAt(0))))
        {
            return !thisType.equals(otherType) ? NEVER :
                   mayBeNull                   ? MAYBE :
                                                 ALWAYS;
        }

        // Strip the class type prefix and suffix of this type, if any.
        if (thisDimensionCount == commonDimensionCount)
        {
            thisType = ClassUtil.internalClassNameFromClassType(thisType);
        }

        // Strip the class type prefix and suffix of the other type, if any.
        if (otherDimensionCount == commonDimensionCount)
        {
            otherType = ClassUtil.internalClassNameFromClassType(otherType);
        }

        // If this type is an array type, and the other type is not
        // java.lang.Object, java.lang.Cloneable, or java.io.Serializable,
        // this type can never be an instance.
        if (thisDimensionCount > otherDimensionCount &&
            !ClassUtil.isInternalArrayInterfaceName(otherType))
        {
            return NEVER;
        }

        // If the other type is an array type, and this type is not
        // java.lang.Object, java.lang.Cloneable, or java.io.Serializable,
        // this type can never be an instance.
        if (thisDimensionCount < otherDimensionCount &&
            !ClassUtil.isInternalArrayInterfaceName(thisType))
        {
            return NEVER;
        }

        // If this type may be null, it might not be an instance of any class.
        if (mayBeNull)
        {
            return MAYBE;
        }

        // If this type is equal to the other type, or if the other type is
        // java.lang.Object, this type is always an instance.
        if (thisType.equals(otherType) ||
            ClassConstants.INTERNAL_NAME_JAVA_LANG_OBJECT.equals(otherType))
        {
            return ALWAYS;
        }

        // If this type is an array type, it's ok.
        if (thisDimensionCount > otherDimensionCount)
        {
            return ALWAYS;
        }

        // If the other type is an array type, it might be ok.
        if (thisDimensionCount < otherDimensionCount)
        {
            return MAYBE;
        }

        // If the value extends the type, we're sure.
        return referencedClass      != null &&
               otherReferencedClass != null &&
               referencedClass.extendsOrImplements(otherReferencedClass) ?
                   ALWAYS :
                   MAYBE;
    }


    public ReferenceValue generalizeMayBeNull(boolean mayBeNull)
    {
        return this.mayBeNull == mayBeNull ?
            this :
            new TypedReferenceValue(type, referencedClass, true);
    }


    public ReferenceValue referenceArrayLoad(IntegerValue indexValue, ValueFactory valueFactory)
    {
        return
            type == null                         ? ValueFactory.REFERENCE_VALUE_NULL                        :
            !ClassUtil.isInternalArrayType(type) ? ValueFactory.REFERENCE_VALUE_JAVA_LANG_OBJECT_MAYBE_NULL :
                                                   valueFactory.createValue(type.substring(1),
                                                                            referencedClass,
                                                                            true).referenceValue();
    }


    // Implementations of binary methods of ReferenceValue.

    public ReferenceValue generalize(ReferenceValue other)
    {
        return other.generalize(this);
    }


    public int equal(ReferenceValue other)
    {
        return other.equal(this);
    }


    // Implementations of binary ReferenceValue methods with TypedReferenceValue
    // arguments.

    public ReferenceValue generalize(TypedReferenceValue other)
    {
        // If both types are identical, the generalization is the same too.
        if (this.equals(other))
        {
            return this;
        }

        String thisType  = this.type;
        String otherType = other.type;

        // If both types are nul, the generalization is null too.
        if (thisType == null && otherType == null)
        {
            return ValueFactory.REFERENCE_VALUE_NULL;
        }

        // If this type is null, the generalization is the other type, maybe null.
        if (thisType == null)
        {
            return other.generalizeMayBeNull(true);
        }

        // If the other type is null, the generalization is this type, maybe null.
        if (otherType == null)
        {
            return this.generalizeMayBeNull(true);
        }

        boolean mayBeNull = this.mayBeNull || other.mayBeNull;

        // If the two types are equal, the generalization remains the same, maybe null.
        if (thisType.equals(otherType))
        {
            return typedReferenceValue(this, mayBeNull);
        }

        // Start taking into account the type dimensions.
        int thisDimensionCount   = ClassUtil.internalArrayTypeDimensionCount(thisType);
        int otherDimensionCount  = ClassUtil.internalArrayTypeDimensionCount(otherType);
        int commonDimensionCount = Math.min(thisDimensionCount, otherDimensionCount);

        if (thisDimensionCount == otherDimensionCount)
        {
            // See if we can take into account the referenced classes.
            Clazz thisReferencedClass  = this.referencedClass;
            Clazz otherReferencedClass = other.referencedClass;

            if (thisReferencedClass  != null &&
                otherReferencedClass != null)
            {
                if (thisReferencedClass.extendsOrImplements(otherReferencedClass))
                {
                    return typedReferenceValue(other, mayBeNull);
                }

                if (otherReferencedClass.extendsOrImplements(thisReferencedClass))
                {
                    return typedReferenceValue(this, mayBeNull);
                }

                // Collect the superclasses and interfaces of this class.
                Set thisSuperClasses = new HashSet();
                thisReferencedClass.hierarchyAccept(false, true, true, false,
                                                    new ClassCollector(thisSuperClasses));

                int thisSuperClassesCount = thisSuperClasses.size();
                if (thisSuperClassesCount == 0 &&
                    thisReferencedClass.getSuperName() != null)
                {
                    throw new IllegalArgumentException("Can't find any super classes of ["+thisType+"] (not even immediate super class ["+thisReferencedClass.getSuperName()+"])");
                }

                // Collect the superclasses and interfaces of the other class.
                Set otherSuperClasses = new HashSet();
                otherReferencedClass.hierarchyAccept(false, true, true, false,
                                                     new ClassCollector(otherSuperClasses));

                int otherSuperClassesCount = otherSuperClasses.size();
                if (otherSuperClassesCount == 0 &&
                    otherReferencedClass.getSuperName() != null)
                {
                    throw new IllegalArgumentException("Can't find any super classes of ["+otherType+"] (not even immediate super class ["+otherReferencedClass.getSuperName()+"])");
                }

                if (DEBUG)
                {
                    System.out.println("ReferenceValue.generalize this ["+thisReferencedClass.getName()+"] with other ["+otherReferencedClass.getName()+"]");
                    System.out.println("  This super classes:  "+thisSuperClasses);
                    System.out.println("  Other super classes: "+otherSuperClasses);
                }

                // Find the common superclasses.
                thisSuperClasses.retainAll(otherSuperClasses);

                if (DEBUG)
                {
                    System.out.println("  Common super classes: "+thisSuperClasses);
                }

                // Find a class that is a subclass of all common superclasses,
                // or that at least has the maximum number of common superclasses.
                Clazz commonClass = null;

                int maximumSuperClassCount = -1;

                // Go over all common superclasses to find it. In case of
                // multiple subclasses, keep the lowest one alphabetically,
                // in order to ensure that the choice is deterministic.
                Iterator commonSuperClasses = thisSuperClasses.iterator();
                while (commonSuperClasses.hasNext())
                {
                    Clazz commonSuperClass = (Clazz)commonSuperClasses.next();

                    int superClassCount = superClassCount(commonSuperClass, thisSuperClasses);
                    if (maximumSuperClassCount < superClassCount ||
                        (maximumSuperClassCount == superClassCount &&
                         commonClass != null                       &&
                         commonClass.getName().compareTo(commonSuperClass.getName()) > 0))
                    {
                        commonClass            = commonSuperClass;
                        maximumSuperClassCount = superClassCount;
                    }
                }

                if (commonClass == null)
                {
                    throw new IllegalArgumentException("Can't find common super class of ["+
                                                       thisType +"] (with "+thisSuperClassesCount +" known super classes) and ["+
                                                       otherType+"] (with "+otherSuperClassesCount+" known super classes)");
                }

                if (DEBUG)
                {
                    System.out.println("  Best common class: ["+commonClass.getName()+"]");
                }

                // TODO: Handle more difficult cases, with multiple global subclasses.

                return new TypedReferenceValue(commonDimensionCount == 0 ?
                                                   commonClass.getName() :
                                                   ClassUtil.internalArrayTypeFromClassName(commonClass.getName(),
                                                                                            commonDimensionCount),
                                               commonClass,
                                               mayBeNull);
            }
        }
        else if (thisDimensionCount > otherDimensionCount)
        {
            // See if the other type is an interface type of arrays.
            if (ClassUtil.isInternalArrayInterfaceName(ClassUtil.internalClassNameFromClassType(otherType)))
            {
                return typedReferenceValue(other, mayBeNull);
            }
        }
        else if (thisDimensionCount < otherDimensionCount)
        {
            // See if this type is an interface type of arrays.
            if (ClassUtil.isInternalArrayInterfaceName(ClassUtil.internalClassNameFromClassType(thisType)))
            {
                return typedReferenceValue(this, mayBeNull);
            }
        }

        // Reduce the common dimension count if either type is an array of
        // primitives type of this dimension.
        if (commonDimensionCount > 0 &&
            (ClassUtil.isInternalPrimitiveType(otherType.charAt(commonDimensionCount))) ||
             ClassUtil.isInternalPrimitiveType(thisType.charAt(commonDimensionCount)))
        {
            commonDimensionCount--;
        }

        // Fall back on a basic Object or array of Objects type.
        return commonDimensionCount == 0 ?
            mayBeNull ?
                ValueFactory.REFERENCE_VALUE_JAVA_LANG_OBJECT_MAYBE_NULL :
                ValueFactory.REFERENCE_VALUE_JAVA_LANG_OBJECT_NOT_NULL   :
            new TypedReferenceValue(ClassUtil.internalArrayTypeFromClassName(ClassConstants.INTERNAL_NAME_JAVA_LANG_OBJECT,
                                                                        commonDimensionCount),
                               null,
                               mayBeNull);
    }


    /**
     * Returns the given reference value that may or may not be null, ensuring
     * that it is a TypedReferenceValue, not a subclass.
     */
    private static ReferenceValue typedReferenceValue(TypedReferenceValue referenceValue,
                                                      boolean             mayBeNull)
    {
        return referenceValue.getClass() == TypedReferenceValue.class ?
            referenceValue.generalizeMayBeNull(mayBeNull) :
            new TypedReferenceValue(referenceValue.type,
                                    referenceValue.referencedClass,
                                    mayBeNull);
    }


    /**
     * Returns if the number of superclasses of the given class in the given
     * set of classes.
     */
    private int superClassCount(Clazz subClass, Set classes)
    {
        int count = 0;

        Iterator iterator = classes.iterator();

        while (iterator.hasNext())
        {
            Clazz clazz = (Clazz)iterator.next();
            if (subClass.extendsOrImplements(clazz))
            {
                count++;
            }
        }

        return count;
    }


    public int equal(TypedReferenceValue other)
    {
        return this.type  == null && other.type == null ? ALWAYS : MAYBE;
    }


    // Implementations of binary ReferenceValue methods with
    // IdentifiedReferenceValue arguments.

    public ReferenceValue generalize(IdentifiedReferenceValue other)
    {
        return generalize((TypedReferenceValue)other);
    }


    public int equal(IdentifiedReferenceValue other)
    {
        return equal((TypedReferenceValue)other);
    }


    // Implementations of binary ReferenceValue methods with
    // ArrayReferenceValue arguments.

    public ReferenceValue generalize(ArrayReferenceValue other)
    {
        return generalize((TypedReferenceValue)other);
    }


    public int equal(ArrayReferenceValue other)
    {
        return equal((TypedReferenceValue)other);
    }


    // Implementations of binary ReferenceValue methods with
    // IdentifiedArrayReferenceValue arguments.

    public ReferenceValue generalize(IdentifiedArrayReferenceValue other)
    {
        return generalize((ArrayReferenceValue)other);
    }


    public int equal(IdentifiedArrayReferenceValue other)
    {
        return equal((ArrayReferenceValue)other);
    }


    // Implementations of binary ReferenceValue methods with
    // DetailedArrayReferenceValue arguments.

    public ReferenceValue generalize(DetailedArrayReferenceValue other)
    {
        return generalize((IdentifiedArrayReferenceValue)other);
    }


    public int equal(DetailedArrayReferenceValue other)
    {
        return equal((IdentifiedArrayReferenceValue)other);
    }


    // Implementations for Value.

    public boolean isParticular()
    {
        return type == null;
    }


    public final String internalType()
    {
        return
            type == null                        ? ClassConstants.INTERNAL_TYPE_JAVA_LANG_OBJECT :
            ClassUtil.isInternalArrayType(type) ? type                                          :
                                                  ClassConstants.INTERNAL_TYPE_CLASS_START +
                                                  type +
                                                  ClassConstants.INTERNAL_TYPE_CLASS_END;
    }


    // Implementations for Object.

    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }

        if (object == null ||
            this.getClass() != object.getClass())
        {
            return false;
        }

        TypedReferenceValue other = (TypedReferenceValue)object;
        return this.type == null ? other.type == null :
                                   (this.mayBeNull == other.mayBeNull &&
                                    this.type.equals(other.type));
    }


    public int hashCode()
    {
        return this.getClass().hashCode() ^
               (type == null ? 0 : type.hashCode() ^ (mayBeNull ? 0 : 1));
    }


    public String toString()
    {
        return type == null ?
            "null" :
            type + (referencedClass == null ? "?" : "") + (mayBeNull ? "" : "!");
    }
}
