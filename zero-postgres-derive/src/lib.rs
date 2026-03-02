use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Meta, parse_macro_input, spanned::Spanned};

/// Derive macro for `FromRow` trait.
///
/// Generates an implementation that matches column names to struct fields.
///
/// # Example
///
/// ```ignore
/// #[derive(FromRow)]
/// struct User {
///     name: String,
///     age: i32,
/// }
/// ```
///
/// # Strict Mode
///
/// By default, unknown columns are silently skipped. Use `#[from_row(strict)]`
/// to error on unknown columns:
///
/// ```ignore
/// #[derive(FromRow)]
/// #[from_row(strict)]
/// struct User {
///     name: String,
///     age: i32,
/// }
/// ```
#[proc_macro_derive(FromRow, attributes(from_row))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Check for #[from_row(strict)]
    let strict = input.attrs.iter().any(|attr| {
        if !attr.path().is_ident("from_row") {
            return false;
        }
        match &attr.meta {
            Meta::List(list) => list.tokens.to_string().contains("strict"),
            _ => false,
        }
    });

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("FromRow only supports structs with named fields"),
        },
        _ => panic!("FromRow only supports structs"),
    };

    let field_names: Vec<_> = fields
        .iter()
        .map(|f| f.ident.as_ref().expect("named fields always have idents"))
        .collect();
    let field_types: Vec<_> = fields.iter().map(|f| &f.ty).collect();
    let field_name_strs: Vec<_> = field_names.iter().map(|n| n.to_string()).collect();

    // Generate MaybeUninit declarations
    let uninit_decls = field_names
        .iter()
        .zip(field_types.iter())
        .map(|(name, ty)| {
            quote! {
                let mut #name: ::core::mem::MaybeUninit<#ty> = ::core::mem::MaybeUninit::uninit();
            }
        });

    // Generate set flags
    let set_flag_names: Vec<_> = field_names
        .iter()
        .map(|n| syn::Ident::new(&format!("{}_set", n), n.span()))
        .collect();

    let set_flag_decls = set_flag_names.iter().map(|flag| {
        quote! { let mut #flag = false; }
    });

    // Generate match arms for text decoding
    let match_arms_text = field_names.iter().zip(field_types.iter()).zip(set_flag_names.iter()).zip(field_name_strs.iter()).map(|(((name, ty), flag), name_str)| {
        quote! {
            #name_str => {
                let __val: #ty = match __value {
                    None => ::zero_postgres::conversion::FromWireValue::from_null()?,
                    Some(__bytes) => ::zero_postgres::conversion::FromWireValue::from_text(__field.type_oid(), __bytes)?,
                };
                #name.write(__val);
                #flag = true;
            }
        }
    });

    // Generate match arms for binary decoding
    let match_arms_binary = field_names.iter().zip(field_types.iter()).zip(set_flag_names.iter()).zip(field_name_strs.iter()).map(|(((name, ty), flag), name_str)| {
        quote! {
            #name_str => {
                let __val: #ty = match __value {
                    None => ::zero_postgres::conversion::FromWireValue::from_null()?,
                    Some(__bytes) => ::zero_postgres::conversion::FromWireValue::from_binary(__field.type_oid(), __bytes)?,
                };
                #name.write(__val);
                #flag = true;
            }
        }
    });

    // Generate fallback arm based on strict mode
    let fallback_arm = if strict {
        quote! {
            __unknown => {
                return Err(::zero_postgres::Error::Decode(format!("unknown column: {}", __unknown)));
            }
        }
    } else {
        quote! {
            _ => {
                // Skip unknown column
            }
        }
    };

    // Generate initialization checks
    let init_checks = field_names
        .iter()
        .zip(set_flag_names.iter())
        .zip(field_name_strs.iter())
        .map(|((_name, flag), name_str)| {
            quote! {
                if !#flag {
                    return Err(::zero_postgres::Error::Decode(format!("missing column: {}", #name_str)));
                }
            }
        });

    // Generate struct construction
    let field_inits = field_names.iter().map(|name| {
        quote! {
            #name: unsafe { #name.assume_init() }
        }
    });

    // Clone iterators for text implementation
    let uninit_decls_text = uninit_decls.clone();
    let set_flag_decls_text = set_flag_decls.clone();
    let init_checks_text = init_checks.clone();
    let field_inits_text = field_inits.clone();

    // Clone for binary implementation
    let uninit_decls_binary = field_names
        .iter()
        .zip(field_types.iter())
        .map(|(name, ty)| {
            quote! {
                let mut #name: ::core::mem::MaybeUninit<#ty> = ::core::mem::MaybeUninit::uninit();
            }
        });

    let set_flag_decls_binary = set_flag_names.iter().map(|flag| {
        quote! { let mut #flag = false; }
    });

    let init_checks_binary = field_names
        .iter()
        .zip(set_flag_names.iter())
        .zip(field_name_strs.iter())
        .map(|((_name, flag), name_str)| {
            quote! {
                if !#flag {
                    return Err(::zero_postgres::Error::Decode(format!("missing column: {}", #name_str)));
                }
            }
        });

    let field_inits_binary = field_names.iter().map(|name| {
        quote! {
            #name: unsafe { #name.assume_init() }
        }
    });

    let expanded = quote! {
        impl #impl_generics ::zero_postgres::conversion::FromRow<'_> for #name #ty_generics #where_clause {
            fn from_row_text(
                __cols: &[::zero_postgres::protocol::backend::query::FieldDescription],
                __row: ::zero_postgres::protocol::backend::query::DataRow<'_>,
            ) -> ::zero_postgres::Result<Self> {
                #(#uninit_decls_text)*
                #(#set_flag_decls_text)*

                let mut __values = __row.iter();

                for __field in __cols.iter() {
                    let __value = __values.next().flatten();
                    let __col_name = __field.name;
                    match __col_name {
                        #(#match_arms_text)*
                        #fallback_arm
                    }
                }

                #(#init_checks_text)*

                Ok(Self {
                    #(#field_inits_text),*
                })
            }

            fn from_row_binary(
                __cols: &[::zero_postgres::protocol::backend::query::FieldDescription],
                __row: ::zero_postgres::protocol::backend::query::DataRow<'_>,
            ) -> ::zero_postgres::Result<Self> {
                #(#uninit_decls_binary)*
                #(#set_flag_decls_binary)*

                let mut __values = __row.iter();

                for __field in __cols.iter() {
                    let __value = __values.next().flatten();
                    let __col_name = __field.name;
                    match __col_name {
                        #(#match_arms_binary)*
                        #fallback_arm
                    }
                }

                #(#init_checks_binary)*

                Ok(Self {
                    #(#field_inits_binary),*
                })
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derive macro for `RefFromRow` trait - zero-copy row decoding.
///
/// This macro generates a zero-copy implementation that returns a reference
/// directly into the row buffer. It also derives zerocopy traits automatically.
///
/// # Requirements
///
/// - Struct must have `#[repr(C, packed)]` attribute
/// - All fields must be `LengthPrefixed<T>` where `T` implements `FixedWireSize`
/// - All columns must be `NOT NULL` (no `Option<T>` support)
/// - Only works with binary format (extended queries)
///
/// # PostgreSQL Wire Format
///
/// PostgreSQL's binary protocol includes a 4-byte length prefix before each
/// column value. Use `LengthPrefixed<T>` to account for this in the struct layout.
///
/// # Example
///
/// ```ignore
/// use zero_postgres::conversion::ref_row::{RefFromRow, LengthPrefixed, I64BE, I32BE};
///
/// #[derive(RefFromRow)]
/// #[repr(C, packed)]
/// struct UserStats {
///     user_id: LengthPrefixed<I64BE>,     // 4 + 8 = 12 bytes
///     login_count: LengthPrefixed<I32BE>, // 4 + 4 = 8 bytes
/// }
/// // Total wire size: 20 bytes
/// ```
#[proc_macro_derive(RefFromRow)]
pub fn derive_ref_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = &input.ident;

    // Check for #[repr(C, packed)]
    let has_repr_c_packed = input.attrs.iter().any(|attr| {
        if !attr.path().is_ident("repr") {
            return false;
        }
        let tokens = match &attr.meta {
            Meta::List(list) => list.tokens.to_string(),
            _ => return false,
        };
        tokens.contains("C") && tokens.contains("packed")
    });

    if !has_repr_c_packed {
        return syn::Error::new(
            input.ident.span(),
            "RefFromRow requires #[repr(C, packed)] on the struct",
        )
        .to_compile_error()
        .into();
    }

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return syn::Error::new(
                    input.ident.span(),
                    "RefFromRow only supports structs with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new(input.ident.span(), "RefFromRow only supports structs")
                .to_compile_error()
                .into();
        }
    };

    let field_types: Vec<_> = fields.iter().map(|f| &f.ty).collect();

    // Generate compile-time assertions that all fields implement FixedWireSize
    let wire_size_checks = field_types.iter().map(|ty| {
        quote! {
            const _: () = {
                // This fails to compile if the type doesn't implement FixedWireSize
                fn __assert_fixed_wire_size<T: ::zero_postgres::conversion::ref_row::FixedWireSize>() {}
                fn __check() { __assert_fixed_wire_size::<#ty>(); }
            };
        }
    });

    // Calculate total wire size at compile time
    let wire_size_sum = field_types.iter().map(|ty| {
        quote! { <#ty as ::zero_postgres::conversion::ref_row::FixedWireSize>::WIRE_SIZE }
    });

    let expanded = quote! {
        // Compile-time checks that all fields implement FixedWireSize
        #(#wire_size_checks)*

        // Derive zerocopy traits for zero-copy access
        unsafe impl ::zerocopy::KnownLayout for #name {}
        unsafe impl ::zerocopy::Immutable for #name {}
        unsafe impl ::zerocopy::FromBytes for #name {}

        impl<'a> ::zero_postgres::conversion::ref_row::RefFromRow<'a> for #name {
            fn ref_from_row_binary(
                _cols: &[::zero_postgres::protocol::backend::query::FieldDescription],
                row: ::zero_postgres::protocol::backend::query::DataRow<'a>,
            ) -> ::zero_postgres::Result<&'a Self> {
                // Expected size (includes length prefixes via LengthPrefixed<T>)
                const EXPECTED_SIZE: usize = 0 #(+ #wire_size_sum)*;

                // Get raw data including length prefixes
                let data = row.raw_data();

                if data.len() < EXPECTED_SIZE {
                    return Err(::zero_postgres::Error::Decode(
                        format!(
                            "Row data too small: expected {} bytes, got {}",
                            EXPECTED_SIZE,
                            data.len()
                        )
                    ));
                }

                ::zerocopy::FromBytes::ref_from_bytes(&data[..EXPECTED_SIZE])
                    .map_err(|e| ::zero_postgres::Error::Decode(
                        format!("RefFromRow zerocopy error: {:?}", e)
                    ))
            }
        }
    };

    TokenStream::from(expanded)
}
