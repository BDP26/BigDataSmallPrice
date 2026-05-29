param(
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$MarkdownPath = Join-Path $ScriptDir "blogartikel.md"
$TemplatePath = Join-Path $ScriptDir "blogartikel.template.tex"
$BuildDir = Join-Path $ScriptDir "_build"
$GeneratedTexPath = Join-Path $BuildDir "blogartikel.tex"
$JobName = "blogartikel"

function Escape-Latex {
    param([string]$Text)

    if ($null -eq $Text) {
        return ""
    }

    $escaped = $Text
    $escaped = $escaped -replace '\\', '\textbackslash{}'
    $escaped = $escaped -replace '&', '\&'
    $escaped = $escaped -replace '%', '\%'
    $escaped = $escaped -replace '\$', '\$'
    $escaped = $escaped -replace '#', '\#'
    $escaped = $escaped -replace '_', '\_'
    $escaped = $escaped -replace '\{', '\{'
    $escaped = $escaped -replace '\}', '\}'
    $escaped = $escaped -replace '~', '\textasciitilde{}'
    $escaped = $escaped -replace '\^', '\textasciicircum{}'
    return $escaped
}

function Convert-InlineMarkdown {
    param([string]$Text)

    $result = Escape-Latex $Text
    $result = $result -replace '\*\*(.+?)\*\*', '\textbf{$1}'
    $result = $result -replace '\*(.+?)\*', '\emph{$1}'
    $result = $result -replace '(https?://[^\s]+)', '\url{$1}'
    return $result
}

function Convert-MarkdownToLatex {
    param([string[]]$Lines)

    $latexLines = New-Object System.Collections.Generic.List[string]
    $inYaml = $false
    $yamlDelimitersSeen = 0
    $inItemize = $false

    foreach ($line in $Lines) {
        if ($line.Trim() -eq "---" -and $yamlDelimitersSeen -lt 2) {
            $yamlDelimitersSeen++
            $inYaml = ($yamlDelimitersSeen -eq 1)
            continue
        }

        if ($inYaml) {
            continue
        }

        if ($line -match '^#\s+') {
            continue
        }

        if ($line -match '^##\s+(.+)$') {
            if ($inItemize) {
                $latexLines.Add("\end{itemize}")
                $inItemize = $false
            }
            $latexLines.Add("\section*{$((Convert-InlineMarkdown $Matches[1]))}")
            continue
        }

        if ($line -match '^\-\s+(.+)$') {
            if (-not $inItemize) {
                $latexLines.Add("\begin{itemize}")
                $inItemize = $true
            }
            $latexLines.Add("  \item $((Convert-InlineMarkdown $Matches[1]))")
            continue
        }

        if ($line -match '^!\[(.*?)\]\((.*)\)$') {
            if ($inItemize) {
                $latexLines.Add("\end{itemize}")
                $inItemize = $false
            }

            $imagePath = $Matches[2].Trim()
            $imagePath = $imagePath.Trim("<", ">")
            $imagePath = $imagePath -replace '%20', ' '
            $quotedImagePath = '"' + $imagePath + '"'
            $latexLines.Add("\begin{center}")
            $latexLines.Add("\includegraphics[width=\linewidth]{$quotedImagePath}")
            $latexLines.Add("\end{center}")
            continue
        }

        if ([string]::IsNullOrWhiteSpace($line)) {
            if ($inItemize) {
                $latexLines.Add("\end{itemize}")
                $inItemize = $false
            }
            $latexLines.Add("")
            continue
        }

        $paragraph = $line.TrimEnd() -replace '\s\s$', ''
        $latexLines.Add((Convert-InlineMarkdown $paragraph))
    }

    if ($inItemize) {
        $latexLines.Add("\end{itemize}")
    }

    return ($latexLines -join [Environment]::NewLine)
}

function Get-YamlValue {
    param(
        [string[]]$Lines,
        [string]$Key
    )

    foreach ($line in $Lines) {
        if ($line -match "^$([regex]::Escape($Key)):\s*`"(.+)`"\s*$") {
            return $Matches[1]
        }
    }
    return ""
}

if ($Clean) {
    Get-ChildItem $ScriptDir -Filter "blogartikel.*" |
        Where-Object { $_.Extension -in ".aux", ".fdb_latexmk", ".fls", ".log", ".out", ".synctex.gz" } |
        Remove-Item -Force
    if (Test-Path $BuildDir) {
        Remove-Item $BuildDir -Recurse -Force
    }
    exit 0
}

$markdownLines = Get-Content $MarkdownPath -Encoding UTF8
$template = Get-Content $TemplatePath -Encoding UTF8 -Raw

$title = Get-YamlValue $markdownLines "title"
$authors = Get-YamlValue $markdownLines "authors"
$content = Convert-MarkdownToLatex $markdownLines

$tex = $template.Replace("@@TITLE@@", (Escape-Latex $title))
$tex = $tex.Replace("@@AUTHORS@@", (Escape-Latex $authors))
$tex = $tex.Replace("@@CONTENT@@", $content)

New-Item -ItemType Directory -Path $BuildDir -Force | Out-Null
Set-Content -Path $GeneratedTexPath -Value $tex -Encoding UTF8

Push-Location $ScriptDir
try {
    pdflatex -interaction=nonstopmode -halt-on-error "-jobname=$JobName" "-output-directory=$ScriptDir" $GeneratedTexPath
    pdflatex -interaction=nonstopmode -halt-on-error "-jobname=$JobName" "-output-directory=$ScriptDir" $GeneratedTexPath
}
finally {
    Pop-Location
}
