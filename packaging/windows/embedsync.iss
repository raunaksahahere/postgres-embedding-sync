; Inno Setup script for the embedsync Windows installer.
; iscc /DVersion=0.2.0 /DVariant=minilm /DSourceDir=<payload> /DOutputDir=<out> embedsync.iss
; Installs per user by default (no administrator rights needed) and adds the
; install directory to the user's PATH so `embedsync` works in any new terminal.

#ifndef Version
  #error Version is required
#endif

[Setup]
AppId={{7B0E7C61-2D6B-4B8E-9C1E-0E5B5A9E4D21}
AppName=embedsync
AppVersion={#Version}
AppVerName=embedsync {#Version} ({#Variant})
AppPublisher=raunaksahahere
AppPublisherURL=https://github.com/raunaksahahere/postgres-embedding-sync
DefaultDirName={autopf}\embedsync
DisableProgramGroupPage=yes
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
ChangesEnvironment=yes
OutputDir={#OutputDir}
OutputBaseFilename=embedsync-{#Version}-{#Variant}-windows-x64-setup
Compression=lzma2/normal
SolidCompression=yes
LicenseFile={#SourceDir}\LICENSE
InfoAfterFile={#SourceDir}\README.txt
UninstallDisplayName=embedsync {#Version} ({#Variant})
WizardStyle=modern

[Files]
Source: "{#SourceDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Registry]
Root: HKA; Subkey: "{code:EnvKey}"; ValueType: expandsz; ValueName: "Path"; ValueData: "{olddata};{app}"; Check: NeedsAddPath(ExpandConstant('{app}'))

[Run]
Filename: "{cmd}"; Parameters: "/K ""cd /d %USERPROFILE% && echo embedsync is installed. Run: embedsync setup"""; Description: "Open a terminal to run embedsync setup"; Flags: postinstall nowait skipifsilent unchecked

[Code]
function EnvKey(Param: string): string;
begin
  if IsAdminInstallMode then
    Result := 'SYSTEM\CurrentControlSet\Control\Session Manager\Environment'
  else
    Result := 'Environment';
end;

function RootKey: Integer;
begin
  if IsAdminInstallMode then Result := HKEY_LOCAL_MACHINE else Result := HKEY_CURRENT_USER;
end;

function NeedsAddPath(Dir: string): Boolean;
var
  Paths: string;
begin
  if not RegQueryStringValue(RootKey, EnvKey(''), 'Path', Paths) then
  begin
    Result := True;
    exit;
  end;
  Result := Pos(';' + Uppercase(Dir) + ';', ';' + Uppercase(Paths) + ';') = 0;
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  Paths, Dir: string;
  P: Integer;
begin
  if CurUninstallStep <> usPostUninstall then exit;
  if not RegQueryStringValue(RootKey, EnvKey(''), 'Path', Paths) then exit;
  Dir := ExpandConstant('{app}');
  P := Pos(';' + Uppercase(Dir), Uppercase(Paths));
  if P > 0 then
  begin
    Delete(Paths, P, Length(Dir) + 1);
    RegWriteExpandStringValue(RootKey, EnvKey(''), 'Path', Paths);
  end;
end;
