package com.ti.utils;

import java.util.HashMap;
import java.util.Map;

public class GenerateLocationJson {
    public Map<String,String> generateMap()
    {
        String eng="Afghanistan, Angola, Albania, Argentina, Armenia, Australia, Austria, Azerbaijan, Burundi, Belgium, Benin, Bangladesh, Bulgaria, Belarus, Belize, Bermuda, Bolivia, Brazil, Brunei, Bhutan, Botswana, Canada, Switzerland, Chile, China, Cameroon, Colombia, Cuba, Cyprus, Germany, Djibouti, Denmark, Algeria, Ecuador, Egypt, Eritrea, Spain, Estonia, Ethiopia, Finland, Fiji, France, Gabon, Georgia, Ghana, Guinea, Gambia, Greece, Greenland, Guatemala, Guyana, Honduras, Croatia, Haiti, Hungary, Indonesia, India, Ireland, Iran, Iraq, Iceland, Israel, Italy, Jamaica, Jordan, Japan, Kazakhstan, Kenya, Kyrgyzstan, Cambodia, Kosovo, Kuwait, Laos, Lebanon, Liberia, Libya, Lesotho, Lithuania, Luxembourg, Latvia, Morocco, Moldova, Madagascar, Mexico, Macedonia, Mali, Myanmar, Montenegro, Mongolia, Mozambique, Mauritania, Malawi, Malaysia, Namibia, Niger, Nigeria, Nicaragua, Netherlands, Norway, Nepal, Oman, Pakistan, Panama, Peru, Philippines, Poland, Portugal, Paraguay, Qatar, Romania, Russia, Rwanda, Sudan, Senegal, Somaliland, Somalia, Suriname, Slovakia, Slovenia, Sweden, Swaziland, Syria, Chad, Togo, Thailand, Tajikistan, Turkmenistan, Tunisia, Turkey, Uganda, Ukraine, Uruguay, Uzbekistan, Venezuela, Vietnam, Vanuatu, Yemen, Zambia, Zimbabwe".replace("，",",");
        String china="阿富汗，安哥拉，阿尔巴尼亚，阿根廷，亚美尼亚，澳大利亚，奥地利，阿塞拜疆，布隆迪，比利时，贝宁，孟加拉国，保加利亚，白俄罗斯，伯利兹，百慕大，玻利维亚，巴西，文莱，不丹，博茨瓦纳，加拿大，瑞士，智利，中国，喀麦隆，哥伦比亚，古巴，塞浦路斯，德国，吉布提，丹麦，阿尔及利亚，厄瓜多尔，埃及，厄立特里亚，西班牙，爱沙尼亚，埃塞俄比亚，芬兰，斐济，法国，加蓬，格鲁吉亚，加纳，几内亚，冈比亚，希腊，格陵兰，危地马拉，圭亚那，洪都拉斯，克罗地亚，海地，匈牙利，印度尼西亚，印度，爱尔兰，伊朗，伊拉克，冰岛，以色列，意大利，牙买加，约旦，日本，哈萨克斯坦，肯尼亚，吉尔吉斯斯坦，柬埔寨，科索沃，科威特，老挝，黎巴嫩，利比里亚，利比亚，莱索托，立陶宛，卢森堡，拉脱维亚，摩洛哥，摩尔多瓦，马达加斯加，墨西哥，马其顿，马里，缅甸，黑山，蒙古，莫桑比克，毛里塔尼亚，马拉维，马来西亚，纳米比亚，尼日尔，尼日利亚，尼加拉瓜，荷兰，挪威，尼泊尔，阿曼，巴基斯坦，巴拿马，秘鲁，菲律宾，波兰，葡萄牙，巴拉圭，卡塔尔，罗马尼亚，俄罗斯，卢旺达，苏丹，塞内加尔，索马里兰，索马里，苏里南，斯洛伐克，斯洛文尼亚，瑞典，斯威士兰，叙利亚，乍得，多哥，泰国，塔吉克斯坦，土库曼斯坦，突尼斯，土耳其，乌干达，乌克兰，乌拉圭，乌兹别克斯坦，委内瑞拉，越南，瓦努阿图，也门, 津巴布韦, 赞比亚".replace("，",",");
        String [] engArray=eng.split(",");
        String [] chinaArray=china.split(",");
        System.out.println();
        HashMap<String,String> convertMap=new HashMap<>();
        for(int i=0;i<engArray.length;i++)
        {
            convertMap.put(chinaArray[i].replaceAll(" ",""),engArray[i].replace(" ",""));
        }
//        System.out.println(convertMap);
        return convertMap;
    }
    public static void main(String[] args) {
        GenerateLocationJson g=new GenerateLocationJson();
        g.generateMap();
    }
}
