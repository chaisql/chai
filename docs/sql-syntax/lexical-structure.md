# Lexical structure

## Identifiers

Identifiers are sequence of characters which refer to table names, field names and index names.

Identifiers may be unquoted or surrounded by backquotes. Depending on that, different rules may apply.

<table>
  <thead>
    <tr>
      <th style="text-align:left">Unquoted identifiers</th>
      <th style="text-align:left">Identifiers surrounded by backquotes </th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left">
        <p></p>
        <ul>
          <li>Must begin with an uppercase or lowercase ASCII character or an underscore</li>
          <li>May contain only ASCII letters, digits and underscore</li>
        </ul>
      </td>
      <td style="text-align:left">
        <p></p>
        <ul>
          <li>May contain any unicode character, other than the new line character (i.e. <code>\n</code>)</li>
          <li>May contain escaped <code>`</code> character (i.e. <code>\`</code>)</li>
        </ul>
      </td>
    </tr>
  </tbody>
</table>

```text
foo
_foo_123_
`頂きます (*｀▽´)_旦~~`
`foo \` bar`
```

## Literals

### Strings

A string is a sequence of characters surrounded by double or single quotes. They may contain any unicode character or escaped single or double quotes \(i.e `\'` or `\"`\)

```text
foo
"l'école des fans"
'(╯ಠ_ಠ）╯︵ ┳━┳'
'foo \''
```

### Integers

An integer is a sequence of characters that only contain digits. They may start with a `+` or `-`sign.

```text
123456789
+100
-455
```

### Floats

A float is a sequence of characters that contains three parts:

- a sequence of digits
- a decimal point \(i.e. `.`\)
- a sequence of digits

They may start with a `+`or a `-`sign.

```text
123.456
+3.14
-1.0
```

### Booleans

A boolean is any sequence of character that is written as `true` or `false`, regardless of the case.

```text
true
false
TRUE
FALSE
tRUe
FALse
```
