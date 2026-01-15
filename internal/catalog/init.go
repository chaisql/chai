package catalog

const (
	InternalSchemaOID   = SchemaID(1)
	PublicSchemaOID     = SchemaID(2)
	DefaultCollationOID = CollationID(3)
	BaseTypeOID         = TypeID(100)
)

const (
	InternalSchemaName   = "chai_catalog"
	PublicSchemaName     = "public"
	DefaultCollationName = "default"
)

func (c *Catalog) init() {
	c.createDefaultSchemas()
	c.createDefaultCollations()
	c.createBooleanType()
	c.createNumericTypes()
	c.createStringTypes()
	c.createUserDefinedTypes()
	c.createDateTimeTypes()
}

func (c *Catalog) createDefaultSchemas() {
	// create internal schema
	c.store.addSchema(Schema{
		OID:  InternalSchemaOID,
		Name: Ident{Raw: InternalSchemaName, Folded: InternalSchemaName, Quoted: false},
	})

	// create public schema
	c.store.addSchema(Schema{
		OID:  PublicSchemaOID,
		Name: Ident{Raw: PublicSchemaName, Folded: PublicSchemaName, Quoted: false},
	})
}

func (c *Catalog) createDefaultCollations() {
	// create default collation
	c.store.addCollation(Collation{
		OID:           DefaultCollationOID,
		SchemaOID:     InternalSchemaOID,
		Name:          DefaultCollationName,
		Locale:        "C",
		Deterministic: true,
	})
}

func (c *Catalog) createBooleanType() {
	// create bool type
	c.store.addType(Type{
		OID:       TypeID(BaseTypeOID + 1),
		SchemaOID: InternalSchemaOID,
		Name:      "bool",
		Category:  TypeCatBool,
		Preferred: true,
	})
}

func (c *Catalog) createNumericTypes() {
	const numericBaseOID = BaseTypeOID + 10

	// create int2 type
	c.store.addType(Type{
		OID:       TypeID(numericBaseOID + 1),
		SchemaOID: InternalSchemaOID,
		Name:      "int2",
		Category:  TypeCatNumeric,
	})

	// create int4 type
	c.store.addType(Type{
		OID:       TypeID(numericBaseOID + 2),
		SchemaOID: InternalSchemaOID,
		Name:      "int4",
		Category:  TypeCatNumeric,
	})

	// create int8 type
	c.store.addType(Type{
		OID:       TypeID(numericBaseOID + 3),
		SchemaOID: InternalSchemaOID,
		Name:      "int8",
		Category:  TypeCatNumeric,
	})

	// create float4 type
	c.store.addType(Type{
		OID:       TypeID(numericBaseOID + 4),
		SchemaOID: InternalSchemaOID,
		Name:      "float4",
		Category:  TypeCatNumeric,
	})

	// create float8 type
	c.store.addType(Type{
		OID:       TypeID(numericBaseOID + 5),
		SchemaOID: InternalSchemaOID,
		Name:      "float8",
		Category:  TypeCatNumeric,
	})

	// create numeric type
	c.store.addType(Type{
		OID:       TypeID(numericBaseOID + 6),
		SchemaOID: InternalSchemaOID,
		Name:      "numeric",
		Category:  TypeCatNumeric,
	})
}

func (c *Catalog) createStringTypes() {
	const stringBaseOID = BaseTypeOID + 30

	// create character type
	c.store.addType(Type{
		OID:                 TypeID(stringBaseOID + 1),
		SchemaOID:           InternalSchemaOID,
		Name:                "char",
		Category:            TypeCatString,
		Collatable:          true,
		DefaultCollationOID: DefaultCollationOID,
	})

	// create character varying type
	c.store.addType(Type{
		OID:                 TypeID(stringBaseOID + 2),
		SchemaOID:           InternalSchemaOID,
		Name:                "varchar",
		Category:            TypeCatString,
		Collatable:          true,
		DefaultCollationOID: DefaultCollationOID,
	})

	// create bpchar type
	c.store.addType(Type{
		OID:                 TypeID(stringBaseOID + 4),
		SchemaOID:           InternalSchemaOID,
		Name:                "bpchar",
		Category:            TypeCatString,
		Collatable:          true,
		DefaultCollationOID: DefaultCollationOID,
	})

	// create text type
	c.store.addType(Type{
		OID:                 TypeID(stringBaseOID + 3),
		SchemaOID:           InternalSchemaOID,
		Name:                "text",
		Category:            TypeCatString,
		Collatable:          true,
		DefaultCollationOID: DefaultCollationOID,
	})
}

func (c *Catalog) createUserDefinedTypes() {
	const userDefinedBaseOID = BaseTypeOID + 40

	// create bytea type
	c.store.addType(Type{
		OID:       TypeID(userDefinedBaseOID + 1),
		SchemaOID: InternalSchemaOID,
		Name:      "bytea",
		Category:  TypeCatUserDefined,
	})

	// create uuid type
	c.store.addType(Type{
		OID:       TypeID(userDefinedBaseOID + 2),
		SchemaOID: InternalSchemaOID,
		Name:      "uuid",
		Category:  TypeCatUserDefined,
	})

	// create json type
	c.store.addType(Type{
		OID:       TypeID(userDefinedBaseOID + 3),
		SchemaOID: InternalSchemaOID,
		Name:      "json",
		Category:  TypeCatUserDefined,
	})

	// create jsonb type
	c.store.addType(Type{
		OID:       TypeID(userDefinedBaseOID + 4),
		SchemaOID: InternalSchemaOID,
		Name:      "jsonb",
		Category:  TypeCatUserDefined,
	})
}

func (c *Catalog) createDateTimeTypes() {
	const dateTimeBaseOID = BaseTypeOID + 50

	// create timestamp without time zone type
	c.store.addType(Type{
		OID:       TypeID(dateTimeBaseOID + 1),
		SchemaOID: InternalSchemaOID,
		Name:      "timestamp",
		Category:  TypeCatTimespan,
	})

	// create timestamp with time zone type
	c.store.addType(Type{
		OID:       TypeID(dateTimeBaseOID + 2),
		SchemaOID: InternalSchemaOID,
		Name:      "timestamptz",
		Category:  TypeCatTimespan,
	})

	// create date type
	c.store.addType(Type{
		OID:       TypeID(dateTimeBaseOID + 3),
		SchemaOID: InternalSchemaOID,
		Name:      "date",
		Category:  TypeCatTimespan,
	})

	// create time without time zone type
	c.store.addType(Type{
		OID:       TypeID(dateTimeBaseOID + 4),
		SchemaOID: InternalSchemaOID,
		Name:      "time",
		Category:  TypeCatTimespan,
	})
}
