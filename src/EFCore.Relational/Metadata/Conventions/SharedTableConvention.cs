// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Metadata.Conventions
{
    /// <summary>
    ///     A convention that manipulates names of database objects for entity types that share a table to avoid clashes.
    /// </summary>
    public class SharedTableConvention : IModelFinalizingConvention
    {
        /// <summary>
        ///     Creates a new instance of <see cref="SharedTableConvention" />.
        /// </summary>
        /// <param name="dependencies"> Parameter object containing dependencies for this convention. </param>
        /// <param name="relationalDependencies">  Parameter object containing relational dependencies for this convention. </param>
        public SharedTableConvention(
            [NotNull] ProviderConventionSetBuilderDependencies dependencies,
            [NotNull] RelationalConventionSetBuilderDependencies relationalDependencies)
        {
            Dependencies = dependencies;
        }

        /// <summary>
        ///     Parameter object containing service dependencies.
        /// </summary>
        protected virtual ProviderConventionSetBuilderDependencies Dependencies { get; }

        /// <inheritdoc />
        public virtual void ProcessModelFinalizing(IConventionModelBuilder modelBuilder, IConventionContext<IConventionModelBuilder> context)
        {
            var maxLength = modelBuilder.Metadata.GetMaxIdentifierLength();
            var tables = new Dictionary<(string TableName, string Schema), ISet<IConventionEntityType>>();

            TryUniquifyTableNames(modelBuilder.Metadata, tables, maxLength);

            var columns = new Dictionary<string, IConventionProperty>(StringComparer.Ordinal);
            var keys = new Dictionary<string, IConventionKey>(StringComparer.Ordinal);
            var foreignKeys = new Dictionary<string, IConventionForeignKey>(StringComparer.Ordinal);
            var indexes = new Dictionary<string, IConventionIndex>(StringComparer.Ordinal);
            foreach (var table in tables)
            {
                columns.Clear();
                keys.Clear();
                foreignKeys.Clear();

                var (tableName, schema) = table.Key;
                foreach (var entityType in table.Value)
                {
                    TryUniquifyColumnNames(entityType, columns, tableName, schema, maxLength);
                    TryUniquifyKeyNames(entityType, keys, tableName, schema, maxLength);
                    TryUniquifyForeignKeyNames(entityType, foreignKeys, tableName, schema, maxLength);
                    TryUniquifyIndexNames(entityType, indexes, tableName, schema, maxLength);
                }
            }
        }

        private static void TryUniquifyTableNames(
            IConventionModel model, Dictionary<(string TableName, string Schema), ISet<IConventionEntityType>> tables, int maxLength)
        {
            foreach (var entityType in model.GetEntityTypes())
            {
                var tableName = (TableName: entityType.GetTableName(), Schema: entityType.GetSchema());
                if (tableName.TableName == null
                    || entityType.FindPrimaryKey() == null)
                {
                    continue;
                }

                if (!tables.TryGetValue(tableName, out var entityTypes))
                {
                    entityTypes = new HashSet<IConventionEntityType>();
                    tables[tableName] = entityTypes;
                }

                entityTypes.Add(entityType);
            }

            Multigraph<IConventionEntityType, object> entityTypeGraph = null;
            List<(ISet<IConventionEntityType> Component, string TableName, string Schema, ISet<IConventionEntityType> OldComponent)> componentsToUniquify = null;
            foreach (var table in tables)
            {
                var (tableName, schema) = table.Key;
                var entityTypes = table.Value;
                if (entityTypes.Count == 1
                    || tableName.Length < maxLength)
                {
                    continue;
                }

                if (entityTypeGraph == null)
                {
                    entityTypeGraph = new Multigraph<IConventionEntityType, object>();
                }
                else
                {
                    entityTypeGraph.Clear();
                }

                foreach (var entityType in entityTypes)
                {
                    entityTypeGraph.AddVertex(entityType);
                }

                foreach (var entityType in entityTypes)
                {
                    var baseEntityType = entityType.BaseType;
                    if (baseEntityType != null
                        && entityTypes.Contains(baseEntityType))
                    {
                        entityTypeGraph.AddEdge(entityType, baseEntityType, null);
                    }

                    foreach (var linkingFk in entityType.FindTableIntrarowForeignKeys(tableName, schema))
                    {
                        entityTypeGraph.AddEdge(entityType, linkingFk.PrincipalEntityType, null);
                    }
                }

                var components = entityTypeGraph.GetWeaklyConnectedComponents();
                var anyComponentSkipped = false;
                for (var i = 1; i < components.Count; i++)
                {
                    var currentComponent = components[i];
                    if (currentComponent.Any(e => e[RelationalAnnotationNames.TableName] != null))
                    {
                        anyComponentSkipped = true;
                        continue;
                    }

                    if (componentsToUniquify == null)
                    {
                        componentsToUniquify = new List<(ISet<IConventionEntityType> Component, string TableName, string Schema, ISet<IConventionEntityType> OldComponent)>();
                    }
                    componentsToUniquify.Add((currentComponent, tableName, schema, entityTypes));
                }

                if (anyComponentSkipped)
                {
                    var firstComponent = components[0];
                    if (componentsToUniquify == null)
                    {
                        componentsToUniquify = new List<(ISet<IConventionEntityType> Component, string TableName, string Schema, ISet<IConventionEntityType> OldComponent)>();
                    }
                    componentsToUniquify.Add((firstComponent, tableName, schema, entityTypes));
                }
            }

            if (componentsToUniquify != null)
            {
                foreach (var componentTuple in componentsToUniquify)
                {
                    var (component, tableName, schema, entityTypes) = componentTuple;
                    Uniquify(component, tableName, schema, entityTypes, tables, maxLength);
                }
            }
        }

        private static void Uniquify(
            ISet<IConventionEntityType> newComponent,
            string tableName,
            string schema,
            ISet<IConventionEntityType> oldComponent,
            Dictionary<(string TableName, string Schema), ISet<IConventionEntityType>> tables,
            int maxLength)
        {
            var uniqueName = Uniquifier.Uniquify(tableName, tables, n => (n, schema), maxLength);
            tables[(uniqueName, schema)] = newComponent;
            foreach (var entityType in newComponent)
            {
                entityType.Builder.ToTable(uniqueName);
                oldComponent.Remove(entityType);
            }
        }

        private static void TryUniquifyColumnNames(
            IConventionEntityType entityType,
            Dictionary<string, IConventionProperty> properties,
            string tableName,
            string schema,
            int maxLength)
        {
            foreach (var property in entityType.GetDeclaredProperties())
            {
                var columnName = property.GetColumnName(tableName, schema);
                if (!properties.TryGetValue(columnName, out var otherProperty))
                {
                    properties[columnName] = property;
                    continue;
                }

                var identifyingMemberInfo = property.PropertyInfo ?? (MemberInfo)property.FieldInfo;
                if (identifyingMemberInfo != null
                    && identifyingMemberInfo.IsSameAs(otherProperty.PropertyInfo ?? (MemberInfo)otherProperty.FieldInfo))
                {
                    continue;
                }

                var usePrefix = property.DeclaringEntityType != otherProperty.DeclaringEntityType
                    || property.IsPrimaryKey()
                    || otherProperty.IsPrimaryKey();
                if (!property.IsPrimaryKey()
                    && !property.IsConcurrencyToken)
                {
                    var newColumnName = TryUniquify(property, columnName, properties, usePrefix, maxLength);
                    if (newColumnName != null)
                    {
                        properties[newColumnName] = property;
                        continue;
                    }
                }

                if (!otherProperty.IsPrimaryKey()
                    && !otherProperty.IsConcurrencyToken)
                {
                    var newColumnName = TryUniquify(otherProperty, columnName, properties, usePrefix, maxLength);
                    if (newColumnName != null)
                    {
                        properties[columnName] = property;
                        properties[newColumnName] = otherProperty;
                    }
                }
            }
        }

        private static string TryUniquify(
            IConventionProperty property, string columnName, Dictionary<string, IConventionProperty> properties, bool usePrefix,
            int maxLength)
        {
            if (property.Builder.CanSetColumnName(null))
            {
                if (usePrefix)
                {
                    var prefix = property.DeclaringEntityType.ShortName();
                    if (!columnName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                    {
                        columnName = prefix + "_" + columnName;
                    }
                }

                columnName = Uniquifier.Uniquify(columnName, properties, maxLength);
                property.Builder.HasColumnName(columnName);
                properties[columnName] = property;
                return columnName;
            }

            return null;
        }

        private static void TryUniquifyKeyNames(
            IConventionEntityType entityType,
            Dictionary<string, IConventionKey> keys,
            string tableName,
            string schema,
            int maxLength)
        {
            foreach (var key in entityType.GetDeclaredKeys())
            {
                var keyName = key.GetName(tableName, schema);
                if (!keys.TryGetValue(keyName, out var otherKey))
                {
                    keys[keyName] = key;
                    continue;
                }

                if (!key.IsPrimaryKey())
                {
                    var newKeyName = TryUniquify(key, keyName, keys, maxLength);
                    if (newKeyName != null)
                    {
                        keys[newKeyName] = key;
                        continue;
                    }
                }

                if (!otherKey.IsPrimaryKey())
                {
                    var newKeyName = TryUniquify(otherKey, keyName, keys, maxLength);
                    if (newKeyName != null)
                    {
                        keys[keyName] = key;
                        keys[newKeyName] = otherKey;
                    }
                }
            }
        }

        private static string TryUniquify<T>(
            IConventionKey key, string keyName, Dictionary<string, T> keys, int maxLength)
        {
            if (key.Builder.CanSetName(null))
            {
                keyName = Uniquifier.Uniquify(keyName, keys, maxLength);
                key.Builder.HasName(keyName);
                return keyName;
            }

            return null;
        }

        private static void TryUniquifyIndexNames(
            IConventionEntityType entityType,
            Dictionary<string, IConventionIndex> indexes,
            string tableName,
            string schema,
            int maxLength)
        {
            foreach (var index in entityType.GetDeclaredIndexes())
            {
                var indexName = index.GetName(tableName, schema);
                if (!indexes.TryGetValue(indexName, out var otherIndex))
                {
                    indexes[indexName] = index;
                    continue;
                }

                if (index.Builder.CanSetName(null))
                {
                    if (index.GetConfigurationSource() == ConfigurationSource.Convention
                        && otherIndex.GetConfigurationSource() == ConfigurationSource.Convention
                        && otherIndex.Builder.CanSetName(null))
                    {
                        var associatedForeignKey = index.DeclaringEntityType.FindDeclaredForeignKeys(index.Properties).FirstOrDefault();
                        var otherAssociatedForeignKey =
                            otherIndex.DeclaringEntityType.FindDeclaredForeignKeys(index.Properties).FirstOrDefault();
                        if (associatedForeignKey != null
                            && otherAssociatedForeignKey != null
                            && associatedForeignKey.GetConstraintName(tableName, schema,
                                associatedForeignKey.PrincipalEntityType.GetTableName(),
                                associatedForeignKey.PrincipalEntityType.GetSchema())
                                == otherAssociatedForeignKey.GetConstraintName(tableName, schema,
                                    otherAssociatedForeignKey.PrincipalEntityType.GetTableName(),
                                    otherAssociatedForeignKey.PrincipalEntityType.GetSchema())
                            && index.AreCompatible(otherIndex, tableName, schema, shouldThrow: false))
                        {
                            continue;
                        }
                    }

                    var newIndexName = TryUniquify(index, indexName, indexes, maxLength);
                    indexes[newIndexName] = index;
                    continue;
                }

                var newOtherIndexName = TryUniquify(otherIndex, indexName, indexes, maxLength);
                if (newOtherIndexName != null)
                {
                    indexes[indexName] = index;
                    indexes[newOtherIndexName] = otherIndex;
                }
            }
        }

        private static string TryUniquify<T>(
            IConventionIndex index, string indexName, Dictionary<string, T> indexes, int maxLength)
        {
            if (index.Builder.CanSetName(null))
            {
                indexName = Uniquifier.Uniquify(indexName, indexes, maxLength);
                index.Builder.HasName(indexName);
                return indexName;
            }

            return null;
        }

        private static void TryUniquifyForeignKeyNames(
            IConventionEntityType entityType,
            Dictionary<string, IConventionForeignKey> foreignKeys,
            string tableName,
            string schema,
            int maxLength)
        {
            foreach (var foreignKey in entityType.GetDeclaredForeignKeys())
            {
                if (foreignKey.DeclaringEntityType.GetTableName() == foreignKey.PrincipalEntityType.GetTableName()
                    && foreignKey.DeclaringEntityType.GetSchema() == foreignKey.PrincipalEntityType.GetSchema())
                {
                    continue;
                }

                var foreignKeyName = foreignKey.GetConstraintName(tableName, schema,
                    foreignKey.PrincipalEntityType.GetTableName(), foreignKey.PrincipalEntityType.GetSchema());
                if (!foreignKeys.TryGetValue(foreignKeyName, out var otherForeignKey))
                {
                    foreignKeys[foreignKeyName] = foreignKey;
                    continue;
                }

                if (foreignKey.Builder.CanSetConstraintName(null))
                {
                    if (otherForeignKey.Builder.CanSetConstraintName(null)
                        && (foreignKey.PrincipalToDependent != null
                            || foreignKey.DependentToPrincipal != null)
                        && (foreignKey.PrincipalToDependent?.GetIdentifyingMemberInfo()).IsSameAs(
                            otherForeignKey.PrincipalToDependent?.GetIdentifyingMemberInfo())
                        && (foreignKey.DependentToPrincipal?.GetIdentifyingMemberInfo()).IsSameAs(
                            otherForeignKey.DependentToPrincipal?.GetIdentifyingMemberInfo())
                        && foreignKey.AreCompatible(otherForeignKey, tableName, schema, shouldThrow: false))
                    {
                        continue;
                    }

                    var newForeignKeyName = TryUniquify(foreignKey, foreignKeyName, foreignKeys, maxLength);
                    foreignKeys[newForeignKeyName] = foreignKey;
                    continue;
                }

                var newOtherForeignKeyName = TryUniquify(otherForeignKey, foreignKeyName, foreignKeys, maxLength);
                if (newOtherForeignKeyName != null)
                {
                    foreignKeys[foreignKeyName] = foreignKey;
                    foreignKeys[newOtherForeignKeyName] = otherForeignKey;
                }
            }
        }

        private static string TryUniquify<T>(
            IConventionForeignKey foreignKey, string foreignKeyName, Dictionary<string, T> foreignKeys, int maxLength)
        {
            if (foreignKey.Builder.CanSetConstraintName(null))
            {
                foreignKeyName = Uniquifier.Uniquify(foreignKeyName, foreignKeys, maxLength);
                foreignKey.Builder.HasConstraintName(foreignKeyName);
                return foreignKeyName;
            }

            return null;
        }
    }
}
