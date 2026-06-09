export function normalizeCityName(name: string): string {
    const replacements: [string, string][] = [
        ['^gov edson', 'governador edison'],
        ['^sto', 'santo'],
        ['^sto ', 'santo '],
        [' sto ', ' santo '],
        ['sra', 'senhora'], 
        ['^dagua ', "d'agua "], 
        [' dagua ', " d'agua "], 
        ['^pto', 'porto'], 
        ['^pta', 'porta'], 
        ['^benjami(m|n)' ,'benjami'], 
        ['^constante', 'constant'],
        ['^cpo', 'campo'],
        [' cpo$', ' campo'],
        ['^gov ', 'governador '],
        ['^soo ', 'sao '],
        ['^sa o ', 'sao '],
        ['^sta', 'santa'],
        [' sta ', ' santa '],
        ['^s ', 'sao '],
        ['^st ', 'santo '],
        ['vituria', 'vitoria'],
        ['bras(a|o)lia', 'brasilia'],
        ['braslia', 'brasilia'],
        ['valparaizo', 'valparaiso'],
        ['goiais', 'goias'],
        ['^at ', 'alto '],
        ['^fra ', 'feira '],
        [' nv ', ' nova '],
        ['^nv ', 'nova '],
        [' fc ', ' francisco '],
        [' mg', 'minas'],
        ['jooo', 'joao'],
        ['^vl ', 'vila '],
        ['^bx ', 'baixa '],
        ['^mte ', 'monte '],
        [' lrg ', ' largo '],
        ['^sj ', 'sao jose '],
        [' vle ', ' vale '],
        ['^sa0 ', 'sao '],
        ['^dr ', 'doutor '],
        ['^prof ', 'professor '],
        ['^eng ', 'engenheiro '],
        ['^vt ', 'vista '],
        ['^jd ', 'jardim '],
        ['^mal candido', 'marechal candido'],
        ['doeste', "d'oeste"],
        [' mte', ' monte'],
        ['impz', 'imperatriz'],
        ['^vsc ', 'visconde '],
        ['^cb ', 'cabo '],
        ['^cmte ', 'comandante '],
        ['brasopolis', 'brazopolis'],
        ['eldorado dos', 'eldorado do'],
    ];
    let normalized = name.toLowerCase().trim();
    for (const [pattern, replacement] of replacements) {
        normalized = normalized.replace(new RegExp(pattern, 'g'), replacement);
    }
    normalized = normalized
        .replaceAll(/[\s\-\_\.\'\"\(\)]+/g, '|')
        .replaceAll('||', '|');
    let splitted = normalized.split('|').filter(part => part);
    splitted = splitted.map(part => `${part}:*`);
    return splitted.join(' & ');

}