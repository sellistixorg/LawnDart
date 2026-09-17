using LawnDart.Analyzers;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;

namespace LawnDart.Analyzers.Tests;

public class EventSchemaAnalyzerTests
{
    [Fact]
    public async Task MissingAttributeAndNonKebabToken_NotInScope()
    {
        await VerifyAsync("""
            public sealed class NotAnEvent : IEvent { }

            [EventTypeName("AuthorRegistered")]
            public sealed class PascalToken : IEvent { }
            """);
    }

    [Fact]
    public async Task OneArgSingleType_NoDiagnostic()
    {
        await VerifyAsync("""
            [EventTypeName("author-registered")]
            public sealed class AuthorRegistered : IEvent { }
            """);
    }

    [Fact]
    public async Task TwoSeparateOneArgFamilies_NoDiagnostic()
    {
        await VerifyAsync("""
            [EventTypeName("author-registered")]
            public sealed class AuthorRegistered : IEvent { }

            [EventTypeName("book-registered")]
            public sealed class BookRegistered : IEvent { }
            """);
    }

    [Fact]
    public async Task SingleVersionedTypeWithoutCurrent_NoDiagnostic()
    {
        await VerifyAsync("""
            [EventTypeName("author-registered", version: 2)]
            public sealed class AuthorRegistered : IEvent { }
            """);
    }

    [Fact]
    public async Task TwoTypesOneCurrentAndCompleteChain_NoDiagnostic()
    {
        await VerifyAsync("""
            [EventTypeName("author-registered", version: 1)]
            public sealed class AuthorRegisteredV1 : IEvent { }

            [EventTypeName("author-registered", version: 2, current: true)]
            public sealed class AuthorRegistered : IEvent { }

            public sealed class AuthorV1ToCurrent : IEventUpcaster<AuthorRegistered, AuthorRegisteredV1>
            {
                public AuthorRegistered Upcast(AuthorRegisteredV1 source) => new();
            }
            """);
    }

    [Fact]
    public async Task DirectHopToCurrent_NoDiagnostic()
    {
        await VerifyAsync("""
            [EventTypeName("author-evolved", version: 1)]
            public sealed class AuthorV1 : IEvent { }

            [EventTypeName("author-evolved", version: 2)]
            public sealed class AuthorV2 : IEvent { }

            [EventTypeName("author-evolved", version: 3, current: true)]
            public sealed class AuthorCurrent : IEvent { }

            public sealed class AuthorV1ToCurrent : IEventUpcaster<AuthorCurrent, AuthorV1>
            {
                public AuthorCurrent Upcast(AuthorV1 source) => new();
            }

            public sealed class AuthorV2ToCurrent : IEventUpcaster<AuthorCurrent, AuthorV2>
            {
                public AuthorCurrent Upcast(AuthorV2 source) => new();
            }
            """);
    }

    [Fact]
    public async Task TwoCurrents_ReportsLDT001()
    {
        await VerifyAsync("""
            [EventTypeName("author-two-current", version: 1, current: true)]
            public sealed class {|LDT001:TwoCurrentV1|} : IEvent { }

            [EventTypeName("author-two-current", version: 2, current: true)]
            public sealed class {|LDT001:TwoCurrentV2|} : IEvent { }
            """);
    }

    [Fact]
    public async Task MultiTypeFamilyWithoutCurrent_ReportsLDT002()
    {
        await VerifyAsync("""
            [EventTypeName("author-no-current", version: 1)]
            public sealed class {|LDT002:NoCurrentV1|} : IEvent { }

            [EventTypeName("author-no-current", version: 2)]
            public sealed class {|LDT002:NoCurrentV2|} : IEvent { }
            """);
    }

    [Fact]
    public async Task IncompleteChain_ReportsLDT003()
    {
        await VerifyAsync("""
            [EventTypeName("author-evolved", version: 1)]
            public sealed class {|LDT003:AuthorV1|} : IEvent { }

            [EventTypeName("author-evolved", version: 2, current: true)]
            public sealed class AuthorCurrent : IEvent { }
            """);
    }

    [Fact]
    public async Task MissingHopInThreeStepChain_ReportsLDT003()
    {
        await VerifyAsync("""
            [EventTypeName("author-evolved", version: 1)]
            public sealed class {|LDT003:AuthorV1|} : IEvent { }

            [EventTypeName("author-evolved", version: 2)]
            public sealed class AuthorV2 : IEvent { }

            [EventTypeName("author-evolved", version: 3, current: true)]
            public sealed class AuthorCurrent : IEvent { }

            public sealed class AuthorV2ToCurrent : IEventUpcaster<AuthorCurrent, AuthorV2>
            {
                public AuthorCurrent Upcast(AuthorV2 source) => new();
            }
            """);
    }

    [Fact]
    public async Task DowncastDoesNotCountAsHop_ReportsLDT003()
    {
        await VerifyAsync("""
            [EventTypeName("author-evolved", version: 1)]
            public sealed class {|LDT003:AuthorV1|} : IEvent { }

            [EventTypeName("author-evolved", version: 2, current: true)]
            public sealed class AuthorCurrent : IEvent { }

            public sealed class AuthorCurrentToV1 : IEventUpcaster<AuthorV1, AuthorCurrent>
            {
                public AuthorV1 Upcast(AuthorCurrent source) => new();
            }
            """);
    }

    [Fact]
    public async Task CrossFamilyHopDoesNotCount_ReportsLDT003()
    {
        await VerifyAsync("""
            [EventTypeName("author-evolved", version: 1)]
            public sealed class {|LDT003:AuthorV1|} : IEvent { }

            [EventTypeName("author-evolved", version: 2, current: true)]
            public sealed class AuthorCurrent : IEvent { }

            [EventTypeName("book-registered")]
            public sealed class BookRegistered : IEvent { }

            public sealed class AuthorToBook : IEventUpcaster<BookRegistered, AuthorV1>
            {
                public BookRegistered Upcast(AuthorV1 source) => new();
            }
            """);
    }

    [Fact]
    public async Task HistoricalTypeInReferencedAssembly_IncompleteChain_ReportsLDT003()
    {
        var referenceImage = await EmitAssemblyAsync(
            Stubs + """
            [EventTypeName("author-registered", version: 1)]
            public sealed class AuthorRegisteredV1 : IEvent { }
            """,
            "DomainEvents");

        var test = CreateTest("""
            using LawnDart;
            using LawnDart.EventStore;

            [EventTypeName("author-registered", version: 2, current: true)]
            public sealed class {|LDT003:AuthorRegistered|} : IEvent { }
            """);
        test.TestState.AdditionalReferences.Add(referenceImage);
        await test.RunAsync();
    }

    [Fact]
    public async Task HistoricalTypeInReferencedAssembly_CompleteChain_NoDiagnostic()
    {
        var referenceImage = await EmitAssemblyAsync(
            Stubs + """
            [EventTypeName("author-registered", version: 1)]
            public sealed class AuthorRegisteredV1 : IEvent { }
            """,
            "DomainEvents");

        var test = CreateTest("""
            using LawnDart;
            using LawnDart.EventStore;

            [EventTypeName("author-registered", version: 2, current: true)]
            public sealed class AuthorRegistered : IEvent { }

            public sealed class AuthorV1ToCurrent : IEventUpcaster<AuthorRegistered, AuthorRegisteredV1>
            {
                public AuthorRegistered Upcast(AuthorRegisteredV1 source) => new();
            }
            """);
        test.TestState.AdditionalReferences.Add(referenceImage);
        await test.RunAsync();
    }

    private static Task VerifyAsync(string source)
        => CreateTest(Stubs + source).RunAsync();

    private static CSharpAnalyzerTest<EventSchemaAnalyzer, DefaultVerifier> CreateTest(string source)
    {
        var test = new CSharpAnalyzerTest<EventSchemaAnalyzer, DefaultVerifier>
        {
            TestCode = source,
            ReferenceAssemblies = ReferenceAssemblies.Net.Net80,
        };
        test.CompilerDiagnostics = CompilerDiagnostics.Errors;
        return test;
    }

    private static async Task<MetadataReference> EmitAssemblyAsync(string source, string assemblyName)
    {
        var references = await ReferenceAssemblies.Net.Net80
            .ResolveAsync(LanguageNames.CSharp, CancellationToken.None);
        var compilation = CSharpCompilation.Create(
            assemblyName,
            [CSharpSyntaxTree.ParseText(source)],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        using var stream = new MemoryStream();
        var emit = compilation.Emit(stream);
        Assert.True(emit.Success, string.Join(Environment.NewLine, emit.Diagnostics));
        stream.Position = 0;
        return MetadataReference.CreateFromImage(stream.ToArray());
    }

    private const string Stubs = """
        using System;
        using LawnDart;
        using LawnDart.EventStore;

        namespace LawnDart
        {
            public interface IEvent { }
        }

        namespace LawnDart.EventStore
        {
            [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
            public sealed class EventTypeNameAttribute : Attribute
            {
                public EventTypeNameAttribute(string name) : this(name, 1, false) { }

                public EventTypeNameAttribute(string name, int version, bool current = false)
                {
                    Name = name;
                    Version = version;
                    Current = current;
                }

                public string Name { get; }
                public int Version { get; }
                public bool Current { get; }
            }

            public interface IEventUpcaster<out TTo, in TFrom>
                where TTo : class, LawnDart.IEvent
                where TFrom : class, LawnDart.IEvent
            {
                TTo Upcast(TFrom source);
            }
        }

        """;
}
