package com.hong.dataanalysis;

public class Analysis {
	
	

	public static void main(String[] args) throws Exception {
//		Step1_delete.run(args[0],args[1]);
//		Step2_Merger.run(args[1], args[2]);
//		Step3_Add.run(args[0], args[2], args[3]);
//		Step4_Sort.run(args[3],  args[4]);
		Step5_NodeNUM.run("step4_sort", "step5_nodenum");
		Step6_AddUNM.run("step4_sort", "step5_nodenum", "step6_addnode");
		Step7_Sort2.run("step6_addnode", "step7_sort2");
		System.exit(0);
	}
}
