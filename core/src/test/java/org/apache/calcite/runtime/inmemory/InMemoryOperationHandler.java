package org.apache.calcite.runtime.inmemory;

import java.util.List;
import java.lang.reflect.Field;
import java.util.Collection;

public class InMemoryOperationHandler {
    public static void update(Collection<?> collection, List<String> updateColumnList, List<String> sourceExpressionList) {
        int size = collection.size();
        List<?> list = (List<?>)collection;
        for (int index = 0; index < size; index++) {
            Object element = list.get(index);
            Class<?> clazz = element.getClass();
            String updateColumn = updateColumnList.get(index);
            String sourceExpression = sourceExpressionList.get(index);
            int targetExpression = Integer.valueOf(sourceExpression);
            try {
                Field field = clazz.getField(updateColumn);
                field.set(element, targetExpression);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
