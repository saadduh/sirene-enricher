#define MyAppName "SIRENE Enricher"
#define MyAppVersion "7.0"
#define MyAppPublisher "Saad Janina"
#define MyAppURL "https://github.com/saadduh/sirene-enricher"
#define MyAppExeName "SIRENE_Enricher.exe"

[Setup]
[Setup]
AppId={{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}/issues
AppUpdatesURL={#MyAppURL}/releases

; -- PATHS --
; {autopf} will point to C:\Program Files because we are using admin mode
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
OutputDir=C:\Users\Utilisateur\Downloads\installer_output
OutputBaseFilename=SIRENE_Enricher_Setup_v{#MyAppVersion}
SetupIconFile=appicon.ico

; -- ADMIN SETTINGS --
; This forces the Windows UAC prompt
PrivilegesRequired=admin
; We remove PrivilegesRequiredOverridesAllowed to avoid the compiler error

; -- APPEARANCE --
AllowNoIcons=yes
Compression=lzma
SolidCompression=yes
WizardStyle=modern

[Languages]
Name: "french";  MessagesFile: "compiler:Languages\French.isl"
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
Source: "appicon.ico"; DestDir: "{app}"; Flags: ignoreversion
Source: "dist\SIRENE_Enricher.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "naf.json"; DestDir: "{app}"; Flags: ignoreversion
Source: "fj.json";  DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{autostartmenu}\{#MyAppName}";                       Filename: "{app}\{#MyAppExeName}"; IconFilename: "{app}\appicon.ico"
Name: "{autostartmenu}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}"; IconFilename: "{app}\appicon.ico"
Name: "{commondesktop}\{#MyAppName}";                       Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon; IconFilename: "{app}\appicon.ico"

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#MyAppName}}"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
Type: filesandordirs; Name: "{app}\.sirene_cache"